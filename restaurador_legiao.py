#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Restaurador Legião
==================

Interface gráfica para:
  1. selecionar um backup PostgreSQL compactado (``.sql.gz``);
  2. extrair o arquivo ``.sql``;
  3. restaurar o dump no servidor usando o cliente ``psql``.

O log da restauração é gravado em ``restore.log`` na mesma pasta do ``.sql``.

Requisitos: Python 3.10+ (com Tkinter) e o cliente psql instalado.
"""

from __future__ import annotations

import ctypes
import gzip
import json
import os
import queue
import shutil
import subprocess
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from urllib.parse import quote

APP_TITLE = "Restaurador Legião — PostgreSQL"
APP_DIR_NAME = "LegiaoRestaurador"
CONFIG_DIR = Path(os.environ.get("APPDATA", str(Path.home()))) / APP_DIR_NAME
CONFIG_FILE = CONFIG_DIR / "config.json"
CHUNK_SIZE = 1024 * 1024
DEFAULT_PORT = "5432"
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


class TaskCancelled(Exception):
    """Levantada quando o usuário cancela uma tarefa."""


@dataclass(frozen=True)
class Connection:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "require"

    def uri(self) -> str:
        """URI sem senha; a senha é passada ao psql via PGPASSWORD."""
        host = normalize_host(self.host)
        auth = quote(self.user, safe="")
        db = quote(self.database, safe="")
        return (
            f"postgresql://{auth}@{host}:{self.port}/{db}"
            f"?sslmode={quote(self.sslmode, safe='')}&connect_timeout=20"
        )

    def display_uri(self) -> str:
        """URI para exibição no log, com a senha mascarada."""
        host = normalize_host(self.host)
        auth = quote(self.user, safe="")
        if self.password:
            auth += ":***"
        db = quote(self.database, safe="")
        return (
            f"postgresql://{auth}@{host}:{self.port}/{db}"
            f"?sslmode={quote(self.sslmode, safe='')}"
        )


def normalize_host(host: str) -> str:
    """Coloca endereços IPv6 entre colchetes, como exige uma URI."""
    host = host.strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if ":" in host:
        return f"[{host}]"
    return host


def find_psql() -> str | None:
    """Localiza o psql no PATH ou nas pastas padrão do PostgreSQL."""
    found = shutil.which("psql")
    if found:
        return found

    candidates: list[Path] = []
    for env_name in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"):
        base = os.environ.get(env_name)
        if not base:
            continue
        pg_dir = Path(base) / "PostgreSQL"
        if pg_dir.is_dir():
            candidates.extend(pg_dir.glob("*/bin/psql.exe"))

    if not candidates:
        return None

    def version_key(path: Path) -> tuple:
        try:
            return tuple(int(part) for part in path.parent.parent.name.split("."))
        except (ValueError, IndexError):
            return (0,)

    candidates.sort(key=version_key, reverse=True)
    return str(candidates[0])


def sql_path_for(gz_path: Path) -> Path:
    """``backup.sql.gz`` -> ``backup.sql``; ``backup.gz`` -> ``backup.sql``."""
    name = gz_path.name
    lower = name.lower()
    if lower.endswith(".sql.gz"):
        return gz_path.with_name(name[:-3])
    if lower.endswith(".gz"):
        return gz_path.with_name(name[:-3] + ".sql")
    return gz_path.with_name(name + ".sql")


def format_bytes(value: float) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TB"


def gzip_uncompressed_size(gz_path: Path) -> int:
    """Lê o tamanho descompactado no rodapé do gzip (ISIZE)."""
    try:
        with open(gz_path, "rb") as handle:
            handle.seek(-4, os.SEEK_END)
            return int.from_bytes(handle.read(4), "little")
    except OSError:
        return 0


def extract_gz(
    gz_path: Path,
    sql_path: Path,
    progress_cb,
    cancel_event: threading.Event,
) -> None:
    """Extrai ``gz_path`` para ``sql_path`` de forma atômica e cancelável."""
    part_path = sql_path.with_name(sql_path.name + ".part")
    total_out = gzip_uncompressed_size(gz_path)
    written = 0
    last_update = 0.0

    try:
        with open(gz_path, "rb") as raw, gzip.GzipFile(fileobj=raw) as source, open(
            part_path, "wb"
        ) as target:
            while True:
                if cancel_event.is_set():
                    raise TaskCancelled()
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                target.write(chunk)
                written += len(chunk)
                now = time.monotonic()
                if now - last_update >= 0.05:
                    last_update = now
                    percent = (written / total_out * 100.0) if total_out else 0.0
                    progress_cb(min(percent, 99.9))
            target.flush()
            os.fsync(target.fileno())

        if cancel_event.is_set():
            raise TaskCancelled()
        os.replace(part_path, sql_path)
        progress_cb(100.0)
    except BaseException:
        try:
            part_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def build_env(conn: Connection) -> dict[str, str]:
    env = os.environ.copy()
    if conn.password:
        env["PGPASSWORD"] = conn.password
    else:
        env.pop("PGPASSWORD", None)
    env["PGCLIENTENCODING"] = "UTF8"
    env["PGCONNECT_TIMEOUT"] = "20"
    env["PGSSLMODE"] = conn.sslmode
    return env


def pump_lines(stream, target_queue: "queue.Queue[str | None]") -> None:
    """Lê a saída do psql linha a linha para uma fila (thread leitora)."""
    try:
        for line in iter(stream.readline, ""):
            target_queue.put(line)
    except Exception:
        pass
    finally:
        try:
            stream.close()
        except Exception:
            pass
        target_queue.put(None)


class RestauradorApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        self._busy = False
        self._closing = False
        self._cancel_enabled = True
        self.cancel_event = threading.Event()
        self.events: "queue.Queue[tuple[str, object]]" = queue.Queue()
        self._proc_lock = threading.Lock()
        self._current_proc: subprocess.Popen | None = None
        self.last_dir: str | None = None
        self.last_log_path: Path | None = None

        self.title(APP_TITLE)
        self.geometry("1020x840")
        self.minsize(900, 700)
        self._configure_style()
        self._create_variables()
        self._build_ui()
        self._load_config()

        self.backup_var.trace_add("write", self._on_backup_changed)
        self._on_backup_changed()
        self._log_startup()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.report_callback_exception = self._report_callback_exception
        self.after(80, self._poll_events)

    # ------------------------------------------------------------------ UI
    def _configure_style(self) -> None:
        style = ttk.Style(self)
        for theme in ("vista", "winnative", "clam"):
            if theme in style.theme_names():
                try:
                    style.theme_use(theme)
                    break
                except tk.TclError:
                    pass

        tkfont.nametofont("TkDefaultFont").configure(family="Segoe UI", size=10)
        tkfont.nametofont("TkTextFont").configure(family="Segoe UI", size=10)
        style.configure("Title.TLabel", font=("Segoe UI Semibold", 18), foreground="#0f172a")
        style.configure("Muted.TLabel", font=("Segoe UI", 9), foreground="#64748b")
        style.configure("Status.TLabel", font=("Segoe UI", 9), foreground="#334155")
        style.configure("TLabelframe.Label", font=("Segoe UI Semibold", 10), foreground="#1e293b")
        style.configure("TButton", padding=(10, 4))

    def _create_variables(self) -> None:
        self.backup_var = tk.StringVar()
        self.host_var = tk.StringVar()
        self.port_var = tk.StringVar(value=DEFAULT_PORT)
        self.database_var = tk.StringVar(value="postgres")
        self.user_var = tk.StringVar(value="postgres")
        self.password_var = tk.StringVar()
        self.ssl_var = tk.BooleanVar(value=True)
        self.show_password_var = tk.BooleanVar(value=False)
        self.psql_var = tk.StringVar(value=find_psql() or "")
        self.status_var = tk.StringVar(value="Pronto.")
        self.destination_var = tk.StringVar(value="Nenhum backup selecionado.")

    def _build_ui(self) -> None:
        header = ttk.Frame(self, padding=(20, 16, 20, 6))
        header.pack(fill="x")
        ttk.Label(header, text="Restaurador Legião", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Selecione o backup .sql.gz, confira a conexão e restaure com psql.",
            style="Muted.TLabel",
        ).pack(anchor="w", pady=(2, 0))

        status_bar = ttk.Frame(self, padding=(20, 6, 20, 8))
        status_bar.pack(side="bottom", fill="x")
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel").pack(side="left")

        container = ttk.Frame(self, padding=(20, 4, 20, 10))
        container.pack(fill="both", expand=True)

        # 1. Backup -------------------------------------------------------
        backup_frame = ttk.LabelFrame(container, text=" 1. Backup (.sql.gz) ", padding=12)
        backup_frame.pack(fill="x", pady=(0, 10))
        backup_row = ttk.Frame(backup_frame)
        backup_row.pack(fill="x")
        self.backup_entry = ttk.Entry(backup_row, textvariable=self.backup_var)
        self.backup_entry.pack(side="left", fill="x", expand=True)
        self.browse_btn = ttk.Button(
            backup_row, text="Selecionar .sql.gz…", command=self._choose_backup
        )
        self.browse_btn.pack(side="left", padx=(8, 0))
        ttk.Label(
            backup_frame, textvariable=self.destination_var, style="Muted.TLabel"
        ).pack(anchor="w", pady=(6, 0))

        # 2. Conexão ------------------------------------------------------
        conn_frame = ttk.LabelFrame(container, text=" 2. Conexão PostgreSQL ", padding=12)
        conn_frame.pack(fill="x", pady=(0, 10))
        fields = ttk.Frame(conn_frame)
        fields.pack(fill="x")
        fields.columnconfigure(0, weight=4)
        fields.columnconfigure(1, weight=0)
        fields.columnconfigure(2, weight=2)
        fields.columnconfigure(3, weight=2)
        fields.columnconfigure(4, weight=3)

        for text, column in (
            ("Host / IPv6", 0),
            ("Porta", 1),
            ("Banco de dados", 2),
            ("Usuário", 3),
            ("Senha", 4),
        ):
            ttk.Label(fields, text=text).grid(
                row=0, column=column, sticky="w", padx=(0, 10)
            )

        ttk.Entry(fields, textvariable=self.host_var).grid(
            row=1, column=0, sticky="ew", padx=(0, 10), pady=(2, 0)
        )
        ttk.Entry(fields, textvariable=self.port_var, width=8).grid(
            row=1, column=1, sticky="ew", padx=(0, 10), pady=(2, 0)
        )
        ttk.Entry(fields, textvariable=self.database_var).grid(
            row=1, column=2, sticky="ew", padx=(0, 10), pady=(2, 0)
        )
        ttk.Entry(fields, textvariable=self.user_var).grid(
            row=1, column=3, sticky="ew", padx=(0, 10), pady=(2, 0)
        )

        password_cell = ttk.Frame(fields)
        password_cell.grid(row=1, column=4, sticky="ew", pady=(2, 0))
        password_cell.columnconfigure(0, weight=1)
        self.password_entry = ttk.Entry(
            password_cell, textvariable=self.password_var, show="*"
        )
        self.password_entry.grid(row=0, column=0, sticky="ew")
        ttk.Checkbutton(
            password_cell,
            text="ver",
            variable=self.show_password_var,
            command=self._toggle_password,
        ).grid(row=0, column=1, padx=(4, 0))

        options = ttk.Frame(conn_frame)
        options.pack(fill="x", pady=(10, 0))
        ttk.Checkbutton(
            options, text="Exigir SSL (sslmode=require)", variable=self.ssl_var
        ).pack(side="left")
        ttk.Label(
            options,
            text="Senha via PGPASSWORD; não é salva.",
            style="Muted.TLabel",
        ).pack(side="left", padx=(16, 0))

        # 3. psql ---------------------------------------------------------
        psql_frame = ttk.LabelFrame(container, text=" 3. Cliente psql ", padding=12)
        psql_frame.pack(fill="x", pady=(0, 10))
        psql_row = ttk.Frame(psql_frame)
        psql_row.pack(fill="x")
        ttk.Entry(psql_row, textvariable=self.psql_var).pack(
            side="left", fill="x", expand=True
        )
        self.detect_btn = ttk.Button(
            psql_row, text="Detectar", command=self._detect_psql
        )
        self.detect_btn.pack(side="left", padx=(8, 0))
        self.psql_browse_btn = ttk.Button(
            psql_row, text="Procurar…", command=self._browse_psql
        )
        self.psql_browse_btn.pack(side="left", padx=(6, 0))

        # 4. Execução -----------------------------------------------------
        exec_frame = ttk.LabelFrame(container, text=" 4. Execução ", padding=12)
        exec_frame.pack(fill="x", pady=(0, 10))
        buttons = ttk.Frame(exec_frame)
        buttons.pack(fill="x")

        self.restore_btn = tk.Button(
            buttons,
            text="Extrair e restaurar",
            command=lambda: self._start("both"),
            bg="#1d4ed8",
            fg="white",
            activebackground="#1e40af",
            activeforeground="white",
            disabledforeground="#cbd5e1",
            relief="flat",
            bd=0,
            padx=18,
            pady=7,
            cursor="hand2",
            font=("Segoe UI Semibold", 10),
        )
        self.restore_btn.pack(side="left")

        self.extract_btn = ttk.Button(
            buttons, text="Somente extrair", command=lambda: self._start("extract")
        )
        self.extract_btn.pack(side="left", padx=(8, 0))

        self.test_btn = ttk.Button(
            buttons, text="Testar conexão", command=lambda: self._start("test")
        )
        self.test_btn.pack(side="left", padx=(8, 0))

        self.cancel_btn = ttk.Button(
            buttons, text="Cancelar", command=self._cancel, state="disabled"
        )
        self.cancel_btn.pack(side="right")

        progress_row = ttk.Frame(exec_frame)
        progress_row.pack(fill="x", pady=(10, 0))
        self.progress = ttk.Progressbar(
            progress_row, mode="determinate", maximum=100.0
        )
        self.progress.pack(side="left", fill="x", expand=True)
        self.progress_label = ttk.Label(progress_row, text="0%", width=7, anchor="e")
        self.progress_label.pack(side="left", padx=(8, 0))

        # Log -------------------------------------------------------------
        log_frame = ttk.LabelFrame(container, text=" Log da operação ", padding=(10, 8))
        log_frame.pack(fill="both", expand=True)
        log_frame.rowconfigure(1, weight=1)
        log_frame.columnconfigure(0, weight=1)

        log_toolbar = ttk.Frame(log_frame)
        log_toolbar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Button(
            log_toolbar, text="Abrir restore.log", command=self._open_log
        ).pack(side="left")
        ttk.Button(
            log_toolbar, text="Abrir pasta", command=self._open_folder
        ).pack(side="left", padx=(6, 0))
        ttk.Button(log_toolbar, text="Limpar", command=self._clear_log).pack(
            side="left", padx=(6, 0)
        )
        ttk.Label(
            log_toolbar,
            text="O psql pode continuar após erros de permissão do Supabase.",
            style="Muted.TLabel",
        ).pack(side="right")

        self.log_text = tk.Text(
            log_frame,
            height=12,
            wrap="word",
            state="disabled",
            bg="#0f172a",
            fg="#e2e8f0",
            insertbackground="#e2e8f0",
            selectbackground="#334155",
            relief="flat",
            font=("Consolas", 9),
            padx=8,
            pady=6,
        )
        log_scroll = ttk.Scrollbar(
            log_frame, orient="vertical", command=self.log_text.yview
        )
        self.log_text.configure(yscrollcommand=log_scroll.set)
        self.log_text.grid(row=1, column=0, sticky="nsew")
        log_scroll.grid(row=1, column=1, sticky="ns")

    # --------------------------------------------------------------- helpers
    def _log_startup(self) -> None:
        self._append_log("Restaurador Legião iniciado.")
        psql = self.psql_var.get().strip()
        if psql:
            self._append_log(f"psql: {psql}")
        else:
            self._append_log(
                "psql NÃO encontrado. Use “Detectar” ou informe o caminho do psql.exe."
            )
        self._append_log(
            "Selecione o backup .sql.gz e preencha host, porta, banco, usuário e senha."
        )

    def _on_backup_changed(self, *_args) -> None:
        raw = self.backup_var.get().strip().strip('"')
        if not raw:
            self.destination_var.set("Nenhum backup selecionado.")
            return
        path = Path(raw)
        sql_path = sql_path_for(path)
        if sql_path.exists():
            self.destination_var.set(f"SQL de destino: {sql_path}  (já existe; será sobrescrito)")
        else:
            self.destination_var.set(f"SQL de destino: {sql_path}")

    def _toggle_password(self) -> None:
        self.password_entry.configure(show="" if self.show_password_var.get() else "*")

    def _choose_backup(self) -> None:
        initial = self.last_dir
        if not initial:
            desktop_legiao = Path.home() / "Desktop" / "Legiao"
            initial = str(desktop_legiao if desktop_legiao.is_dir() else Path.home())
        selected = filedialog.askopenfilename(
            parent=self,
            title="Selecione o backup PostgreSQL (.sql.gz)",
            initialdir=initial,
            filetypes=[
                ("Backup PostgreSQL", "*.sql.gz"),
                ("Arquivos GZip", "*.gz"),
                ("Todos os arquivos", "*.*"),
            ],
        )
        if selected:
            self.last_dir = str(Path(selected).parent)
            self.backup_var.set(selected)
            self.status_var.set("Backup selecionado.")

    def _detect_psql(self) -> None:
        found = find_psql()
        if found:
            self.psql_var.set(found)
            self._append_log(f"psql encontrado: {found}")
            self.status_var.set("psql localizado.")
        else:
            self._append_log("[aviso] psql não encontrado automaticamente.")
            messagebox.showwarning(
                APP_TITLE,
                "psql não encontrado.\n\n"
                "Instale o PostgreSQL ou informe manualmente o caminho do psql.exe "
                "(por exemplo, C:\\Program Files\\PostgreSQL\\17\\bin\\psql.exe).",
                parent=self,
            )

    def _browse_psql(self) -> None:
        base = Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "PostgreSQL"
        selected = filedialog.askopenfilename(
            parent=self,
            title="Selecione o psql.exe",
            initialdir=str(base if base.is_dir() else Path.home()),
            filetypes=[("psql", "psql.exe"), ("Executáveis", "*.exe"), ("Todos", "*.*")],
        )
        if selected:
            self.psql_var.set(selected)

    def _clear_log(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def _append_log(self, text: object) -> None:
        value = str(text)
        self.log_text.configure(state="normal")
        if self.log_text.index("end-1c") != "1.0":
            self.log_text.insert("end", "\n")
        self.log_text.insert("end", value)
        try:
            line_count = int(self.log_text.index("end-1c").split(".")[0])
            if line_count > 20000:
                self.log_text.delete("1.0", f"{line_count - 15000}.0")
        except (tk.TclError, ValueError):
            pass
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _open_log(self) -> None:
        raw = self.backup_var.get().strip().strip('"')
        path = sql_path_for(Path(raw)).parent / "restore.log" if raw else None
        if path is None:
            path = self.last_log_path
        if path is None or not path.exists():
            messagebox.showinfo(
                APP_TITLE, "O restore.log ainda não foi gerado.", parent=self
            )
            return
        try:
            os.startfile(str(path))
        except OSError as exc:
            messagebox.showerror(APP_TITLE, f"Não foi possível abrir o log:\n{exc}", parent=self)

    def _open_folder(self) -> None:
        raw = self.backup_var.get().strip().strip('"')
        folder = Path(raw).parent if raw else Path.cwd()
        try:
            os.startfile(str(folder))
        except OSError as exc:
            messagebox.showerror(APP_TITLE, f"Não foi possível abrir a pasta:\n{exc}", parent=self)

    # ------------------------------------------------------------ validação
    def _collect_connection(self) -> Connection:
        host = self.host_var.get().strip()
        if not host:
            raise ValueError("Informe o host do PostgreSQL.")
        if "://" in host:
            raise ValueError(
                "No campo Host informe apenas o endereço, sem 'postgresql://'."
            )
        if any(char.isspace() for char in host):
            raise ValueError("O host não pode conter espaços.")

        port_text = self.port_var.get().strip() or DEFAULT_PORT

        # Aceita [IPv6]:porta e host:porta.
        if host.startswith("[") and "]" in host:
            inside, _, rest = host[1:].partition("]")
            host = inside
            if rest.startswith(":"):
                port_text = rest[1:] or port_text
        elif host.count(":") == 1:
            maybe_host, maybe_port = host.rsplit(":", 1)
            if maybe_port.isdigit():
                host, port_text = maybe_host, maybe_port

        if not host:
            raise ValueError("Host inválido.")
        if not port_text.isdigit():
            raise ValueError("A porta precisa ser um número (ex.: 5432).")
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise ValueError("A porta precisa estar entre 1 e 65535.")

        database = self.database_var.get().strip()
        if not database:
            raise ValueError("Informe o nome do banco de dados.")
        user = self.user_var.get().strip()
        if not user:
            raise ValueError("Informe o usuário do PostgreSQL.")

        return Connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=self.password_var.get(),
            sslmode="require" if self.ssl_var.get() else "prefer",
        )

    def _collect_backup(self) -> Path:
        raw = self.backup_var.get().strip().strip('"')
        if not raw:
            raise ValueError("Selecione o arquivo de backup .sql.gz.")
        path = Path(raw)
        if not path.is_file():
            raise ValueError(f"Arquivo não encontrado:\n{path}")
        if path.suffix.lower() != ".gz":
            raise ValueError("O arquivo selecionado não termina em .gz.")
        try:
            with open(path, "rb") as handle:
                magic = handle.read(2)
        except OSError as exc:
            raise ValueError(f"Não foi possível ler o arquivo:\n{exc}") from exc
        if magic != b"\x1f\x8b":
            raise ValueError("O arquivo não parece ser um gzip válido.")
        return path

    def _resolve_psql(self) -> str:
        value = self.psql_var.get().strip().strip('"')
        if value:
            candidate = Path(value)
            if candidate.is_file():
                return str(candidate)
            found = shutil.which(value)
            if found:
                return found
        found = find_psql()
        if found:
            self.psql_var.set(found)
            return found
        raise ValueError(
            "psql não encontrado.\n\n"
            "Instale o PostgreSQL ou informe o caminho completo do psql.exe "
            "(ex.: C:\\Program Files\\PostgreSQL\\17\\bin\\psql.exe)."
        )

    # --------------------------------------------------------------- ações
    def _start(self, mode: str) -> None:
        if self._busy:
            return

        try:
            psql = self._resolve_psql() if mode in ("both", "test") else ""
            conn = self._collect_connection() if mode in ("both", "test") else None
            gz = self._collect_backup() if mode in ("both", "extract") else None
        except ValueError as exc:
            messagebox.showwarning(APP_TITLE, str(exc), parent=self)
            return

        sql_path = sql_path_for(gz) if gz else None
        if sql_path is not None and sql_path.exists():
            overwrite = messagebox.askyesno(
                "Sobrescrever arquivo",
                f"O arquivo SQL já existe:\n\n{sql_path}\n\nDeseja sobrescrevê-lo?",
                parent=self,
            )
            if not overwrite:
                return

        self.cancel_event.clear()
        self._cancel_enabled = mode != "test"
        self._set_busy(True)
        self._progress_determinate()
        self._append_log("")
        self._append_log("=" * 78)
        if mode == "test":
            self.status_var.set("Testando conexão…")
        elif mode == "extract":
            self.status_var.set("Extraindo backup…")
        else:
            self.status_var.set("Extraindo e restaurando…")

        threading.Thread(
            target=self._worker,
            args=(mode, conn, psql, gz, sql_path),
            daemon=True,
        ).start()

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        state = "disabled" if busy else "normal"
        for widget in (
            self.restore_btn,
            self.extract_btn,
            self.test_btn,
            self.browse_btn,
            self.detect_btn,
            self.psql_browse_btn,
        ):
            widget.configure(state=state)
        self.cancel_btn.configure(
            state="normal" if busy and self._cancel_enabled else "disabled"
        )

    def _cancel(self) -> None:
        if not self._busy:
            return
        self.cancel_event.set()
        self.status_var.set("Cancelando…")
        self._kill_current()

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _emit(self, kind: str, payload: object = None) -> None:
        self.events.put((kind, payload))

    # ------------------------------------------------------------- worker
    def _worker(
        self,
        mode: str,
        conn: Connection | None,
        psql: str,
        gz: Path | None,
        sql_path: Path | None,
    ) -> None:
        started = time.monotonic()
        try:
            if mode in ("extract", "both"):
                assert gz is not None and sql_path is not None
                self._emit("progress_mode", "determinate")
                self._emit("progress", 0.0)
                self._emit("status", f"Extraindo {gz.name}…")
                self._emit("log", f"[{self._timestamp()}] Extraindo: {gz}")
                self._emit("log", f"[{self._timestamp()}] Destino: {sql_path}")

                extract_gz(
                    gz,
                    sql_path,
                    lambda percent: self._emit("progress", percent),
                    self.cancel_event,
                )
                size = sql_path.stat().st_size
                self._emit("progress", 100.0)
                self._emit("log", f"[ok] SQL extraído ({format_bytes(size)}).")

                if mode == "extract":
                    self._emit(
                        "done",
                        {"mode": "extract", "sql": str(sql_path), "size": size},
                    )
                    return
                self._emit("status", "Extração concluída. Iniciando restauração…")

            if mode == "test":
                assert conn is not None
                self._do_test(conn, psql)
                return

            assert conn is not None and sql_path is not None
            self._do_restore(conn, psql, sql_path, started)
        except TaskCancelled:
            self._emit("cancelled", {"mode": mode})
        except Exception as exc:  # noqa: BLE001
            self._emit("error", str(exc))

    def _do_test(self, conn: Connection, psql: str) -> None:
        cmd = [
            psql,
            "-w",
            "-X",
            "-A",
            "-t",
            conn.uri(),
            "-c",
            "select current_database() || ' | ' || current_user || ' | ' || version();",
        ]
        self._emit("progress_mode", "indeterminate")
        self._emit("status", "Testando conexão…")
        self._emit("log", f"[{self._timestamp()}] Testando conexão…")
        self._emit("log", f"$ {subprocess.list2cmdline(cmd)}")
        self._emit("log", f"Destino: {conn.display_uri()}")

        try:
            result = subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=build_env(conn),
                timeout=45,
                creationflags=CREATE_NO_WINDOW,
            )
        except subprocess.TimeoutExpired:
            self._emit("test_fail", "Tempo esgotado (45 s) ao tentar conectar.")
            return
        except OSError as exc:
            self._emit("test_fail", f"Não foi possível executar o psql:\n{exc}")
            return

        output = (result.stdout or "").strip()
        if output:
            for line in output.splitlines():
                self._emit("log", line)

        if result.returncode == 0:
            self._emit("log", "[ok] Conexão realizada com sucesso.")
            self._emit("test_ok", output or "Conexão realizada com sucesso.")
        else:
            self._emit("log", f"[falha] psql retornou código {result.returncode}.")
            self._emit(
                "test_fail",
                output or f"psql retornou código {result.returncode}.",
            )

    def _do_restore(
        self,
        conn: Connection,
        psql: str,
        sql_path: Path,
        started: float,
    ) -> None:
        if self.cancel_event.is_set():
            raise TaskCancelled()

        log_path = sql_path.parent / "restore.log"
        self.last_log_path = log_path
        cmd = [psql, "-w", "-X", conn.uri(), "-f", str(sql_path)]
        env = build_env(conn)
        errors = 0
        first_error = ""
        cancelled = False

        try:
            log_handle = open(
                log_path, "w", encoding="utf-8", errors="replace", newline=""
            )
        except OSError as exc:
            raise RuntimeError(
                f"Não foi possível criar o log em {log_path}:\n{exc}"
            ) from exc

        with log_handle:
            header = "\n".join(
                [
                    "=" * 78,
                    "Restaurador Legião — restauração PostgreSQL",
                    f"Data/hora : {self._timestamp()}",
                    f"Backup    : {self.backup_var.get().strip()}",
                    f"SQL       : {sql_path}",
                    f"Destino   : {conn.display_uri()}",
                    f"psql      : {psql}",
                    f"Comando   : {subprocess.list2cmdline(cmd)}",
                    "=" * 78,
                    "",
                ]
            )
            log_handle.write(header)
            log_handle.flush()
            self._emit("log", header.rstrip())

            try:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    env=env,
                    creationflags=CREATE_NO_WINDOW,
                )
            except OSError as exc:
                raise RuntimeError(
                    f"Não foi possível executar o psql:\n{exc}"
                ) from exc

            with self._proc_lock:
                self._current_proc = proc

            line_queue: "queue.Queue[str | None]" = queue.Queue()
            reader = threading.Thread(
                target=pump_lines, args=(proc.stdout, line_queue), daemon=True
            )
            reader.start()

            self._emit("progress_mode", "indeterminate")
            self._emit("status", "Restaurando…")
            self._emit(
                "log",
                f"[{self._timestamp()}] Restauração iniciada. Log: {log_path}",
            )
            self._emit("log", f"$ {subprocess.list2cmdline(cmd)}")

            def handle_line(line: str) -> None:
                nonlocal errors, first_error
                text = line.rstrip("\r\n")
                log_handle.write(line if line.endswith("\n") else line + "\n")
                log_handle.flush()
                self._emit("log", text)
                if "ERROR:" in text:
                    errors += 1
                    if not first_error:
                        first_error = text.strip()[:500]

            last_status = 0.0
            while True:
                if self.cancel_event.is_set():
                    cancelled = True
                    self._emit("status", "Cancelando…")
                    self._terminate_process(proc)
                    break

                if proc.poll() is not None and not reader.is_alive():
                    while True:
                        try:
                            line = line_queue.get_nowait()
                        except queue.Empty:
                            break
                        if line is not None:
                            handle_line(line)
                    break

                try:
                    line = line_queue.get(timeout=0.2)
                except queue.Empty:
                    now = time.monotonic()
                    if now - last_status >= 0.5:
                        last_status = now
                        self._emit(
                            "status",
                            f"Restaurando… {int(now - started)}s | {errors} erro(s)",
                        )
                    continue

                if line is None:
                    if proc.poll() is not None:
                        break
                    continue

                handle_line(line)
                now = time.monotonic()
                if now - last_status >= 0.5:
                    last_status = now
                    self._emit(
                        "status",
                        f"Restaurando… {int(now - started)}s | {errors} erro(s)",
                    )

            try:
                rc = proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                rc = proc.wait(timeout=5)

            with self._proc_lock:
                self._current_proc = None
            elapsed = time.monotonic() - started

            if cancelled:
                note = f"[cancelado] Restauração interrompida em {elapsed:.1f}s."
                log_handle.write("\n" + note + "\n")
                log_handle.flush()
                self._emit("log", note)
                self._emit(
                    "cancelled",
                    {"mode": "restore", "log": str(log_path)},
                )
                return

            summary = (
                f"[{self._timestamp()}] psql finalizado com código {rc} "
                f"em {elapsed:.1f}s — {errors} erro(s) no log."
            )
            log_handle.write("\n" + summary + "\n")
            log_handle.flush()
            self._emit("log", summary)
            self._emit(
                "done",
                {
                    "mode": "restore",
                    "rc": rc,
                    "errors": errors,
                    "first_error": first_error,
                    "log": str(log_path),
                    "sql": str(sql_path),
                    "elapsed": elapsed,
                },
            )

    # ----------------------------------------------------------- processo
    def _terminate_process(self, proc: subprocess.Popen | None) -> None:
        if proc is None or proc.poll() is not None:
            return
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                creationflags=CREATE_NO_WINDOW,
            )
        except Exception:  # noqa: BLE001
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass

    def _kill_current(self) -> None:
        with self._proc_lock:
            proc = self._current_proc
        self._terminate_process(proc)

    # ----------------------------------------------------------- progresso
    def _progress_determinate(self) -> None:
        self.progress.stop()
        self.progress.configure(mode="determinate")
        self.progress["value"] = 0
        self.progress_label.configure(text="0%")

    def _progress_indeterminate(self) -> None:
        self.progress.configure(mode="indeterminate")
        self.progress.start(15)
        self.progress_label.configure(text="…")

    def _set_progress(self, value: object) -> None:
        try:
            number = max(0.0, min(100.0, float(value)))
        except (TypeError, ValueError):
            return
        self.progress.stop()
        self.progress.configure(mode="determinate")
        self.progress["value"] = number
        self.progress_label.configure(text=f"{number:.0f}%")

    def _finish_progress(self, success: bool = True) -> None:
        self.progress.stop()
        self.progress.configure(mode="determinate")
        self.progress["value"] = 100 if success else 0
        self.progress_label.configure(text="100%" if success else "—")

    # --------------------------------------------------------------- eventos
    def _poll_events(self) -> None:
        if self._closing:
            return
        while True:
            try:
                kind, payload = self.events.get_nowait()
            except queue.Empty:
                break
            try:
                self._handle_event(kind, payload)
            except Exception as exc:  # noqa: BLE001
                self._append_log(f"[erro interno] {exc}")
        self.after(80, self._poll_events)

    def _handle_event(self, kind: str, payload: object) -> None:
        if kind == "log":
            self._append_log(payload)
        elif kind == "status":
            self.status_var.set(str(payload))
        elif kind == "progress":
            self._set_progress(payload)
        elif kind == "progress_mode":
            if payload == "indeterminate":
                self._progress_indeterminate()
            else:
                self._progress_determinate()
        elif kind == "done":
            self._on_done(payload)
        elif kind == "error":
            self._on_error(payload)
        elif kind == "cancelled":
            self._on_cancelled(payload)
        elif kind == "test_ok":
            self._on_test_ok(payload)
        elif kind == "test_fail":
            self._on_test_fail(payload)

    def _on_done(self, payload: object) -> None:
        self._set_busy(False)
        self._finish_progress(True)
        data = payload if isinstance(payload, dict) else {}

        if data.get("mode") == "extract":
            sql = str(data.get("sql", ""))
            size = data.get("size", 0)
            try:
                size_text = format_bytes(float(size))
            except (TypeError, ValueError):
                size_text = "?"
            self.status_var.set("Extração concluída.")
            messagebox.showinfo(
                "Extração concluída",
                f"Arquivo SQL gerado:\n\n{sql}\n\nTamanho: {size_text}",
                parent=self,
            )
            return

        rc = int(data.get("rc", -1))
        errors = int(data.get("errors", 0))
        log = str(data.get("log", ""))
        elapsed = float(data.get("elapsed", 0.0))
        first_error = str(data.get("first_error", "")).strip()

        if rc != 0:
            self.status_var.set(f"Restauração com falha (código {rc}).")
            messagebox.showerror(
                "Restauração com falha",
                f"O psql terminou com código {rc}.\n\n"
                f"{first_error or 'Veja o log para mais detalhes.'}\n\nLog: {log}",
                parent=self,
            )
        elif errors:
            self.status_var.set(f"Restauração finalizada com {errors} erro(s).")
            messagebox.showwarning(
                "Restauração concluída com avisos",
                f"O psql terminou, mas o log contém {errors} erro(s).\n\n"
                "Erros de permissão e de event trigger são comuns no Supabase "
                "e não interrompem o psql.\n\n"
                f"Primeiro erro:\n{first_error or '(veja o log)'}\n\nLog: {log}",
                parent=self,
            )
        else:
            self.status_var.set("Restauração concluída sem erros.")
            messagebox.showinfo(
                "Restauração concluída",
                f"Dump restaurado sem erros em {elapsed:.1f}s.\n\nLog: {log}",
                parent=self,
            )

    def _on_error(self, payload: object) -> None:
        self._set_busy(False)
        self._finish_progress(False)
        self.status_var.set("Erro.")
        text = str(payload)
        self._append_log(f"[ERRO] {text}")
        messagebox.showerror(APP_TITLE, text, parent=self)

    def _on_cancelled(self, _payload: object) -> None:
        self._set_busy(False)
        self._finish_progress(False)
        self.status_var.set("Operação cancelada.")
        self._append_log("[cancelado] Operação cancelada pelo usuário.")

    def _on_test_ok(self, payload: object) -> None:
        self._set_busy(False)
        self._finish_progress(True)
        self.status_var.set("Conexão OK.")
        text = str(payload).strip()
        self._append_log("[ok] Conexão testada com sucesso.")
        messagebox.showinfo(
            "Conexão OK",
            text[:1200] or "Conexão realizada com sucesso.",
            parent=self,
        )

    def _on_test_fail(self, payload: object) -> None:
        self._set_busy(False)
        self._finish_progress(False)
        self.status_var.set("Falha na conexão.")
        text = str(payload).strip()
        self._append_log("[falha] Não foi possível conectar.")
        messagebox.showerror(
            "Falha na conexão",
            text[:2000] or "Não foi possível conectar ao PostgreSQL.",
            parent=self,
        )

    def _report_callback_exception(self, exc_type, exc_value, exc_tb) -> None:
        import traceback

        details = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        self._append_log(details)
        messagebox.showerror(
            APP_TITLE, f"Erro inesperado:\n\n{exc_value}", parent=self
        )

    # --------------------------------------------------------------- config
    def _load_config(self) -> None:
        try:
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return
        if not isinstance(data, dict):
            return

        for key, var in (
            ("host", self.host_var),
            ("port", self.port_var),
            ("database", self.database_var),
            ("user", self.user_var),
            ("psql_path", self.psql_var),
        ):
            value = data.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            if key == "psql_path" and not Path(value).is_file():
                continue
            var.set(value)

        self.ssl_var.set(bool(data.get("sslmode_require", True)))
        last_dir = data.get("last_dir")
        if isinstance(last_dir, str) and Path(last_dir).is_dir():
            self.last_dir = last_dir

    def _save_config(self) -> None:
        data = {
            "host": self.host_var.get().strip(),
            "port": self.port_var.get().strip(),
            "database": self.database_var.get().strip(),
            "user": self.user_var.get().strip(),
            "psql_path": self.psql_var.get().strip(),
            "sslmode_require": bool(self.ssl_var.get()),
            "last_dir": self.last_dir or "",
        }
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            CONFIG_FILE.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            pass

    def _on_close(self) -> None:
        if self._busy:
            if not messagebox.askyesno(
                APP_TITLE,
                "Uma operação está em andamento. Deseja cancelar e sair?",
                parent=self,
            ):
                return
            self.cancel_event.set()
            self._kill_current()
            self._closing = True
            self._save_config()
            self.after(400, self.destroy)
            return

        self._closing = True
        self._save_config()
        self.destroy()


def main() -> None:
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:  # noqa: BLE001
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:  # noqa: BLE001
            pass

    try:
        app = RestauradorApp()
        app.mainloop()
    except Exception as exc:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        try:
            messagebox.showerror(
                APP_TITLE,
                f"Não foi possível iniciar o aplicativo:\n\n{exc}",
            )
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()

