import json
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import pandas as pd

try:
    import pyodbc
except ImportError:
    pyodbc = None


APP_NAME = "ShortsheetBuilder"
CONFIG_FILE = "config.json"

# Your mapping
MACHINE_OSSID = "O"
MACHINE_REPAK = "R"


# -----------------------------
# Config model
# -----------------------------
@dataclass
class AppConfig:
    server: str = ""
    database: str = "WPL"
    driver: str = ""
    auth_mode: str = "windows"  # "windows" or "sql"
    username: str = ""
    password: str = ""
    product_excel_path: str = "Product Info.xlsx"
    product_sheet_name: str = ""
    output_folder: str = ""
    exclude_missing_master: bool = True
    exclude_frozen_y: bool = False


def load_config() -> AppConfig:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            merged = {**AppConfig().__dict__, **(data or {})}
            return AppConfig(**merged)
        except Exception:
            return AppConfig()
    return AppConfig()


def save_config(cfg: AppConfig) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg.__dict__, f, indent=2)


# -----------------------------
# SQL helpers
# -----------------------------
def list_odbc_drivers() -> list[str]:
    if pyodbc is None:
        return []
    try:
        drivers = pyodbc.drivers()
        preferred, other = [], []
        for d in drivers:
            if "SQL Server" in d or "ODBC Driver" in d:
                preferred.append(d)
            else:
                other.append(d)
        return preferred[::-1] + other
    except Exception:
        return []


def build_connection_string(cfg: AppConfig) -> str:
    if not cfg.driver:
        raise ValueError("ODBC Driver is blank. Select an installed SQL Server driver.")
    if not cfg.server:
        raise ValueError("Server Address is blank.")

    base = f"DRIVER={{{cfg.driver}}};SERVER={cfg.server};DATABASE={cfg.database};"

    if cfg.auth_mode == "windows":
        return base + "Trusted_Connection=yes;"
    else:
        if not cfg.username:
            raise ValueError("SQL username is blank.")
        return base + f"UID={cfg.username};PWD={cfg.password};"


def test_connection(cfg: AppConfig) -> tuple[bool, str]:
    if pyodbc is None:
        return False, "pyodbc is not installed. Run: pip install pyodbc"
    try:
        cs = build_connection_string(cfg)
        with pyodbc.connect(cs, timeout=8) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True, "Connection successful."
    except Exception as e:
        return False, f"{e}"


def _escape_sql_literal(s: str) -> str:
    return s.replace("'", "''")


def _build_in_list_sql(values: list[str]) -> str:
    return ", ".join("'" + _escape_sql_literal(v) + "'" for v in values)


def sql_int_expr(col_name: str) -> str:
    return f"(CASE WHEN ISNUMERIC({col_name}) = 1 THEN CAST({col_name} AS INT) ELSE NULL END)"


def sql_num_expr(col_name: str) -> str:
    return f"(CASE WHEN ISNUMERIC({col_name}) = 1 THEN CAST({col_name} AS FLOAT) ELSE 0 END)"


def safe_float(x) -> float:
    try:
        if pd.isna(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


# -----------------------------
# Product master
# -----------------------------
def load_product_master(path: str, sheet_name: str = "") -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()

    if sheet_name.strip():
        df = pd.read_excel(path, sheet_name=sheet_name.strip())
    else:
        df = pd.read_excel(path)

    df.columns = [str(c).strip() for c in df.columns]

    if "PLU" in df.columns:
        df["PLU"] = df["PLU"].astype(str).str.strip().str.zfill(5)
    else:
        raise ValueError("Product master must contain a column named 'PLU'.")

    df["DESC"] = df["DESC"].astype(str).fillna("") if "DESC" in df.columns else ""
    df["Trays"] = pd.to_numeric(df["Trays"], errors="coerce").fillna(0).astype(float) if "Trays" in df.columns else 0.0
    df["TPM"] = pd.to_numeric(df["TPM"], errors="coerce").fillna(0).astype(float) if "TPM" in df.columns else 0.0
    df["Machine"] = df["Machine"].astype(str).str.strip() if "Machine" in df.columns else ""
    df["Type"] = df["Type"].astype(str).str.strip() if "Type" in df.columns else ""
    df["Frozen"] = df["Frozen"].astype(str).str.strip().str.upper() if "Frozen" in df.columns else ""

    return df


def product_machines(master_df: pd.DataFrame) -> list[str]:
    if master_df.empty or "Machine" not in master_df.columns:
        return ["All"]
    machines = sorted({str(x).strip() for x in master_df["Machine"].dropna().unique() if str(x).strip()})
    return ["All"] + machines


# -----------------------------
# Shortsheet SQL (TxnDate)
# Remaining = Ordered - Shipped - AvailableCases
# -----------------------------
def fetch_shortsheets(conn, schedule_date: date, wip_statuses: list[str], only_remaining: bool) -> pd.DataFrame:
    if not wip_statuses:
        wip_statuses = ["Available"]

    status_sql = _build_in_list_sql(wip_statuses)

    d_plu = sql_int_expr("d.ItemRef_FullName")
    s_plu = sql_int_expr("s.ProductNumber")
    w_plu = sql_int_expr("w.plu")

    d_qty = sql_num_expr("d.Quantity")
    s_qty = sql_num_expr("s.QtyShipped")

    remaining_expr = "(oa.QtyOrdered - ISNULL(sa.QtyShipped,0) - ISNULL(ia.AvailableCases,0))"
    where_remaining = f"WHERE {remaining_expr} > 0" if only_remaining else ""

    sql = f"""
    WITH OrdersForDay AS (
        SELECT so.TxnID
        FROM WPL.dbo.GP_SalesOrder so
        WHERE so.TxnDate >= CAST(? AS DATETIME)
          AND so.TxnDate <  DATEADD(day, 1, CAST(? AS DATETIME))
    ),
    OrderedAgg AS (
        SELECT
            {d_plu} AS PLU_Int,
            SUM({d_qty}) AS QtyOrdered
        FROM WPL.dbo.GP_SalesOrderLineDetail d
        JOIN OrdersForDay o ON o.TxnID = d.TxnIDKey
        WHERE {d_plu} IS NOT NULL
        GROUP BY {d_plu}
    ),
    ShippedAgg AS (
        SELECT
            {s_plu} AS PLU_Int,
            SUM({s_qty}) AS QtyShipped
        FROM WPL.dbo.Shipped s
        JOIN OrdersForDay o ON o.TxnID = s.OrderNum
        WHERE {s_plu} IS NOT NULL
        GROUP BY {s_plu}
    ),
    InvAgg AS (
        SELECT
            {w_plu} AS PLU_Int,
            COUNT(*) AS AvailableCases
        FROM WPL.dbo.Wip w
        JOIN OrderedAgg oa ON oa.PLU_Int = {w_plu}
        WHERE w.status IN ({status_sql})
          AND {w_plu} IS NOT NULL
        GROUP BY {w_plu}
    )
    SELECT
        oa.PLU_Int,
        oa.QtyOrdered,
        ISNULL(sa.QtyShipped,0) AS QtyShipped,
        ISNULL(ia.AvailableCases,0) AS AvailableCases,
        {remaining_expr} AS RemainingCases
    FROM OrderedAgg oa
    LEFT JOIN ShippedAgg sa ON sa.PLU_Int = oa.PLU_Int
    LEFT JOIN InvAgg ia ON ia.PLU_Int = oa.PLU_Int
    {where_remaining}
    ORDER BY RemainingCases DESC, oa.PLU_Int;
    """

    df = pd.read_sql(sql, conn, params=(schedule_date.isoformat(), schedule_date.isoformat()))
    if df.empty:
        return df

    df["ProductNumber"] = df["PLU_Int"].astype(int)
    df["PLU"] = df["PLU_Int"].astype(int).astype(str).str.zfill(5)
    df = df.drop(columns=["PLU_Int"])
    df["RemainingCases"] = df["RemainingCases"].apply(lambda x: x if safe_float(x) > 0 else 0)

    return df[["ProductNumber", "PLU", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]]


# -----------------------------
# Production SQL (PackDate) includes Shipped
# -----------------------------
def fetch_production_by_packdate(conn, packdate_value, statuses: list[str]) -> pd.DataFrame:
    if not statuses:
        statuses = ["Available", "ScanningSalesOrder", "WaitingToBeInvoiced", "Shipped"]

    status_sql = _build_in_list_sql(statuses)
    w_plu = sql_int_expr("w.plu")
    w_wt = sql_num_expr("w.casewt")

    sql = f"""
    SELECT
        {w_plu} AS PLU_Int,
        COUNT(*) AS CasesProduced,
        SUM({w_wt}) AS LbsProduced
    FROM WPL.dbo.Wip w
    WHERE w.packdate = ?
      AND w.status IN ({status_sql})
      AND {w_plu} IS NOT NULL
    GROUP BY {w_plu}
    ORDER BY {w_plu};
    """

    df = pd.read_sql(sql, conn, params=(packdate_value,))
    if df.empty:
        return df

    df["ProductNumber"] = df["PLU_Int"].astype(int)
    df["PLU"] = df["PLU_Int"].astype(int).astype(str).str.zfill(5)
    df = df.drop(columns=["PLU_Int"])
    return df


# -----------------------------
# Export helpers
# -----------------------------
def export_to_excel(df: pd.DataFrame, out_path: str, sheet_name: str) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def export_multi_to_excel(tables: dict[str, pd.DataFrame], out_path: str) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, df in tables.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)


# -----------------------------
# Time helpers
# -----------------------------
def parse_hhmm(s: str) -> time:
    return datetime.strptime((s or "").strip(), "%H:%M").time()


def minutes_between(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds() / 60.0)


def _end_datetime_for_today(start_dt: datetime, end_t: time) -> datetime:
    end_dt = datetime.combine(start_dt.date(), end_t)
    # allow shift that crosses midnight
    if end_dt < start_dt:
        end_dt = end_dt + timedelta(days=1)
    return end_dt


# -----------------------------
# UI helper: Treeview
# -----------------------------
class TableView(ttk.Frame):
    def __init__(self, parent, columns: list[str], height: int = 12):
        super().__init__(parent)
        self.columns = columns
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=height)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        self.vsb.grid(row=0, column=1, sticky="ns")
        self.hsb.grid(row=1, column=0, sticky="ew")

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        for c in columns:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120, anchor="w")

    def set_dataframe(self, df: pd.DataFrame):
        for item in self.tree.get_children():
            self.tree.delete(item)

        if df is None or df.empty:
            return

        cols = list(df.columns)
        if cols != self.columns:
            self.columns = cols
            self.tree.configure(columns=cols)
            for c in cols:
                self.tree.heading(c, text=c)
                self.tree.column(c, width=120, anchor="w")

        for _, row in df.iterrows():
            values = [row.get(c, "") for c in self.columns]
            self.tree.insert("", "end", values=values)


# -----------------------------
# Dashboard widgets
# -----------------------------
class KpiBlock(tk.Frame):
    def __init__(self, parent, title: str, value_color="#00ff00"):
        super().__init__(parent, bg="#000000")
        self.title = tk.Label(self, text=title, bg="#000000", fg="#00ff00", font=("Segoe UI", 18, "bold"))
        self.value = tk.Label(self, text="—", bg="#000000", fg=value_color, font=("Segoe UI", 22, "bold"))
        self.title.pack(anchor="center", pady=(2, 0))
        self.value.pack(anchor="center", pady=(0, 8))

    def set_value(self, text: str):
        self.value.configure(text=text)


class RatioBlock(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg="#000000")
        tk.Label(self, text=title, bg="#000000", fg="#00ff00", font=("Segoe UI", 16, "bold")).pack(pady=(0, 6))

        grid = tk.Frame(self, bg="#000000")
        grid.pack()

        tk.Label(grid, text="OSSID", bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, padx=18)
        tk.Label(grid, text="REPAK", bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold")).grid(row=0, column=1, padx=18)

        self.ossid = tk.Label(grid, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 16, "bold"))
        self.repak = tk.Label(grid, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 16, "bold"))
        self.ossid.grid(row=1, column=0, padx=18)
        self.repak.grid(row=1, column=1, padx=18)

    def set_values(self, ossid_val: str, repak_val: str):
        self.ossid.configure(text=ossid_val)
        self.repak.configure(text=repak_val)


class DualRateBlock(tk.Frame):
    """Small block to show machine-specific rate (e.g., trays/min)."""
    def __init__(self, parent, title: str):
        super().__init__(parent, bg="#000000", highlightbackground="#00ff00", highlightthickness=1)
        tk.Label(self, text=title, bg="#000000", fg="#00ff00", font=("Segoe UI", 14, "bold")).pack(pady=(8, 6))

        inner = tk.Frame(self, bg="#000000")
        inner.pack(padx=16, pady=(0, 12))

        left = tk.Frame(inner, bg="#000000")
        right = tk.Frame(inner, bg="#000000")
        left.grid(row=0, column=0, padx=22)
        right.grid(row=0, column=1, padx=22)

        tk.Label(left, text="OSSID", bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold")).pack()
        self.ossid = tk.Label(left, text="—", bg="#000000", fg="#ff2b2b", font=("Segoe UI", 14, "bold"))
        self.ossid.pack()

        tk.Label(right, text="REPAK", bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold")).pack()
        self.repak = tk.Label(right, text="—", bg="#000000", fg="#ff2b2b", font=("Segoe UI", 14, "bold"))
        self.repak.pack()

    def set_values(self, ossid_val: str, repak_val: str):
        self.ossid.configure(text=ossid_val)
        self.repak.configure(text=repak_val)


# -----------------------------
# Scrollable frame helper
# -----------------------------
class ScrollableFrame(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)

        self.canvas = tk.Canvas(self, highlightthickness=0)
        self.vsb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.inner = ttk.Frame(self.canvas)
        self.window_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _on_inner_configure(self, _event):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfigure(self.window_id, width=event.width)

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")


# -----------------------------
# Settings popup
# -----------------------------
class SettingsWindow(tk.Toplevel):
    def __init__(self, parent, cfg: AppConfig, on_apply_callback):
        super().__init__(parent)
        self.title("Settings")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()

        self.on_apply_callback = on_apply_callback

        self.var_server = tk.StringVar(value=cfg.server)
        self.var_database = tk.StringVar(value=cfg.database)
        self.var_driver = tk.StringVar(value=cfg.driver)
        self.var_auth = tk.StringVar(value=cfg.auth_mode)
        self.var_user = tk.StringVar(value=cfg.username)
        self.var_pass = tk.StringVar(value=cfg.password)

        self.var_product_path = tk.StringVar(value=cfg.product_excel_path)
        self.var_product_sheet = tk.StringVar(value=cfg.product_sheet_name)
        self.var_output_folder = tk.StringVar(value=cfg.output_folder or os.getcwd())

        self.var_exclude_missing = tk.BooleanVar(value=cfg.exclude_missing_master)
        self.var_exclude_frozen = tk.BooleanVar(value=cfg.exclude_frozen_y)

        pad = {"padx": 10, "pady": 6}

        frm = ttk.Frame(self)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        r = 0
        ttk.Label(frm, text="Server:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_server, width=30).grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Database:").grid(row=r, column=2, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_database, width=10).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="Driver:").grid(row=r, column=0, sticky="w", **pad)
        drivers = list_odbc_drivers()
        self.cmb_driver = ttk.Combobox(frm, textvariable=self.var_driver, values=drivers, width=28)
        self.cmb_driver.grid(row=r, column=1, sticky="w", **pad)
        if drivers and not self.var_driver.get():
            self.var_driver.set(drivers[0])

        ttk.Label(frm, text="Auth:").grid(row=r, column=2, sticky="w", **pad)
        self.cmb_auth = ttk.Combobox(frm, textvariable=self.var_auth, values=["windows", "sql"], width=10, state="readonly")
        self.cmb_auth.grid(row=r, column=3, sticky="w", **pad)
        self.cmb_auth.bind("<<ComboboxSelected>>", lambda e: self._refresh_auth_state())

        r += 1
        ttk.Label(frm, text="User:").grid(row=r, column=0, sticky="w", **pad)
        self.ent_user = ttk.Entry(frm, textvariable=self.var_user, width=20)
        self.ent_user.grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Pass:").grid(row=r, column=2, sticky="w", **pad)
        self.ent_pass = ttk.Entry(frm, textvariable=self.var_pass, width=18, show="*")
        self.ent_pass.grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="Product Excel:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_product_path, width=55).grid(row=r, column=1, columnspan=2, sticky="w", **pad)
        ttk.Button(frm, text="Browse", command=self._browse_product).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="Output Folder:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_output_folder, width=55).grid(row=r, column=1, columnspan=2, sticky="w", **pad)
        ttk.Button(frm, text="Browse", command=self._browse_output).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Checkbutton(frm, text="Exclude PLUs missing from Product Info.xlsx", variable=self.var_exclude_missing).grid(
            row=r, column=0, columnspan=2, sticky="w", **pad
        )
        ttk.Checkbutton(frm, text="Exclude Frozen Items (Frozen = Y)", variable=self.var_exclude_frozen).grid(
            row=r, column=2, columnspan=2, sticky="w", **pad
        )

        r += 1
        btns = ttk.Frame(frm)
        btns.grid(row=r, column=0, columnspan=4, sticky="e", padx=10, pady=(10, 0))

        ttk.Button(btns, text="Test Connection", command=self._test_connection).pack(side="left", padx=8)
        ttk.Button(btns, text="Save", command=self._save).pack(side="left", padx=8)
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="left", padx=8)

        self._refresh_auth_state()

    def _refresh_auth_state(self):
        is_sql = self.var_auth.get().strip().lower() == "sql"
        state = "normal" if is_sql else "disabled"
        self.ent_user.configure(state=state)
        self.ent_pass.configure(state=state)

    def _browse_product(self):
        path = filedialog.askopenfilename(
            title="Select Product Info Excel",
            filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*.*")]
        )
        if path:
            self.var_product_path.set(path)

    def _browse_output(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.var_output_folder.set(folder)

    def _test_connection(self):
        cfg = self._build_cfg()
        ok, msg = test_connection(cfg)
        if ok:
            messagebox.showinfo("Connection", msg, parent=self)
        else:
            messagebox.showerror("Connection", msg, parent=self)

    def _build_cfg(self) -> AppConfig:
        return AppConfig(
            server=self.var_server.get().strip(),
            database=self.var_database.get().strip() or "WPL",
            driver=self.var_driver.get().strip(),
            auth_mode=self.var_auth.get().strip().lower(),
            username=self.var_user.get().strip(),
            password=self.var_pass.get(),
            product_excel_path=self.var_product_path.get().strip(),
            product_sheet_name=self.var_product_sheet.get().strip(),
            output_folder=self.var_output_folder.get().strip(),
            exclude_missing_master=bool(self.var_exclude_missing.get()),
            exclude_frozen_y=bool(self.var_exclude_frozen.get()),
        )

    def _save(self):
        cfg = self._build_cfg()
        try:
            save_config(cfg)
            self.on_apply_callback(cfg)
            messagebox.showinfo("Settings", "Saved.", parent=self)
        except Exception as e:
            messagebox.showerror("Settings", str(e), parent=self)


# -----------------------------
# Main App
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1500x920")

        self.cfg = load_config()

        self.master_df: pd.DataFrame | None = None
        self.last_shortsheets_df: pd.DataFrame | None = None
        self.last_production_df: pd.DataFrame | None = None

        self.var_exclude_missing = tk.BooleanVar(value=self.cfg.exclude_missing_master)
        self.var_exclude_frozen = tk.BooleanVar(value=self.cfg.exclude_frozen_y)

        # Shortsheet inputs
        self.var_txndate = tk.StringVar(value=date.today().isoformat())
        self.var_only_remaining = tk.BooleanVar(value=True)
        self.var_wip_available = tk.BooleanVar(value=True)
        self.var_wip_scanning = tk.BooleanVar(value=True)
        self.var_wip_waiting = tk.BooleanVar(value=True)

        # Production inputs
        self.var_packdate = tk.StringVar(value="")
        self.var_machine = tk.StringVar(value="All")
        self.var_start_time = tk.StringVar(value="08:30")
        self.var_end_time = tk.StringVar(value="17:00")

        self.production_statuses = ["Available", "ScanningSalesOrder", "WaitingToBeInvoiced", "Shipped"]

        self._build_ui()
        self._log("Ready.")
        self._load_master(initial=True)

    def _build_ui(self):
        topbar = ttk.Frame(self)
        topbar.pack(fill="x", padx=10, pady=(10, 6))

        ttk.Button(topbar, text="⚙ Settings…", command=self.open_settings).pack(side="left")
        ttk.Label(topbar, text="   ").pack(side="left")
        ttk.Button(topbar, text="Refresh Dashboard", command=self.refresh_dashboard).pack(side="left")

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_dash = ttk.Frame(self.nb)
        self.dash_scroll = ScrollableFrame(self.tab_dash)
        self.dash_scroll.pack(fill="both", expand=True)

        self.dash_root = tk.Frame(self.dash_scroll.inner, bg="#000000")
        self.dash_root.pack(fill="both", expand=True)

        self.tab_short = ttk.Frame(self.nb)
        self.tab_prod = ttk.Frame(self.nb)

        self.nb.add(self.tab_dash, text="Summary Dashboard")
        self.nb.add(self.tab_short, text="Shortsheet (TxnDate)")
        self.nb.add(self.tab_prod, text="Production (PackDate)")

        self._build_tab_dashboard()
        self._build_tab_shortsheets()
        self._build_tab_production()

        frm_log = ttk.LabelFrame(self, text="Log")
        frm_log.pack(fill="both", expand=False, padx=10, pady=(0, 10))
        self.txt_log = tk.Text(frm_log, height=8, wrap="word")
        self.txt_log.pack(fill="both", expand=True, padx=10, pady=10)

    def open_settings(self):
        SettingsWindow(self, self.cfg, self._apply_settings)

    def _apply_settings(self, cfg: AppConfig):
        self.cfg = cfg
        self.var_exclude_missing.set(cfg.exclude_missing_master)
        self.var_exclude_frozen.set(cfg.exclude_frozen_y)
        self._log("Settings applied.")
        self._load_master(initial=True)

    def _log(self, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.txt_log.insert("end", f"[{ts}] {msg}\n")
        self.txt_log.see("end")
        self.update_idletasks()

    def _parse_txndate(self) -> date:
        return datetime.strptime(self.var_txndate.get().strip(), "%Y-%m-%d").date()

    def _get_short_wip_statuses(self) -> list[str]:
        statuses = []
        if self.var_wip_available.get():
            statuses.append("Available")
        if self.var_wip_scanning.get():
            statuses.append("ScanningSalesOrder")
        if self.var_wip_waiting.get():
            statuses.append("WaitingToBeInvoiced")
        return statuses or ["Available"]

    def _load_master(self, initial: bool = False):
        try:
            path = (self.cfg.product_excel_path or "").strip()
            sheet = (self.cfg.product_sheet_name or "").strip()
            if not path or not os.path.exists(path):
                self.master_df = pd.DataFrame()
                if hasattr(self, "cmb_machine"):
                    self.cmb_machine.configure(values=["All"])
                    self.var_machine.set("All")
                if not initial:
                    messagebox.showwarning("Product Master", "Product Excel not found. Set it in Settings.")
                return

            self._log(f"Loading product master: {path}")
            self.master_df = load_product_master(path, sheet)
            self._log(f"Loaded product master rows: {len(self.master_df):,}")

            machines = product_machines(self.master_df)
            if hasattr(self, "cmb_machine"):
                self.cmb_machine.configure(values=machines)
                if self.var_machine.get() not in machines:
                    self.var_machine.set("All")
        except Exception as e:
            self._log(f"ERROR loading product master: {e}")
            self._log(traceback.format_exc())
            self.master_df = pd.DataFrame()
            if not initial:
                messagebox.showerror("Product Master Error", str(e))

    # ---------------- Dashboard ----------------
    def _build_tab_dashboard(self):
        root = self.dash_root

        self.lbl_dash_time = tk.Label(root, text="—", bg="#000000", fg="#00ff00", font=("Segoe UI", 10, "bold"))
        self.lbl_dash_time.place(relx=0.98, rely=0.02, anchor="ne")

        row1 = tk.Frame(root, bg="#000000")
        row1.pack(fill="x", padx=30, pady=(25, 10))

        self.kpi_traypack = KpiBlock(row1, "TRAY PACK")
        self.kpi_traysmin = KpiBlock(row1, "TRAYS/MIN")
        self.kpi_trays_to_complete = KpiBlock(row1, "TRAYS TO COMPLETE", value_color="#ff2b2b")

        self.kpi_traypack.pack(side="left", padx=40)
        self.kpi_traysmin.pack(side="left", padx=40)

        # NEW: trays/min by machine (your red box area)
        self.block_traysmin_by_machine = DualRateBlock(row1, "TRAYS/MIN BY MACHINE")
        self.block_traysmin_by_machine.pack(side="left", padx=40)

        self.kpi_trays_to_complete.pack(side="right", padx=40)

        row2 = tk.Frame(root, bg="#000000")
        row2.pack(fill="x", padx=30, pady=(0, 10))

        self.tray_completed = RatioBlock(row2, "RUN RATIO (TRAYS) - COMPLETED")
        self.tray_remaining = RatioBlock(row2, "RUN RATIO (TRAYS) - REMAINING")
        self.tray_completed.pack(side="left", padx=60)
        self.tray_remaining.pack(side="left", padx=60)

        row3 = tk.Frame(root, bg="#000000")
        row3.pack(fill="x", padx=30, pady=(10, 10))

        self.kpi_tpcases = KpiBlock(row3, "TP CASES")
        self.kpi_casesmin = KpiBlock(row3, "CASES/MIN")
        self.kpi_cases_to_complete = KpiBlock(row3, "CASES TO COMPLETE", value_color="#ff2b2b")

        self.kpi_tpcases.pack(side="left", padx=40)
        self.kpi_casesmin.pack(side="left", padx=40)
        self.kpi_cases_to_complete.pack(side="right", padx=40)

        row4 = tk.Frame(root, bg="#000000")
        row4.pack(fill="x", padx=30, pady=(0, 10))

        self.case_completed = RatioBlock(row4, "CASE RATIO - COMPLETED")
        self.case_remaining = RatioBlock(row4, "CASE RATIO - REMAINING")
        self.case_completed.pack(side="left", padx=60)
        self.case_remaining.pack(side="left", padx=60)

        sep = tk.Frame(root, bg="#00ff00", height=2)
        sep.pack(fill="x", padx=10, pady=(10, 10))

        lbl = tk.Label(root, text="ESTIMATED TIME OF COMPLETION", bg="#000000", fg="#00ff00",
                       font=("Segoe UI", 22, "bold"))
        lbl.pack(pady=(0, 8))

        self.lbl_estimates = tk.Label(root, text="Run Shortsheet + Production then Refresh Dashboard.",
                                      bg="#000000", fg="#00ff00", font=("Segoe UI", 12, "bold"),
                                      justify="left")
        self.lbl_estimates.pack(anchor="w", padx=60, pady=(0, 6))

        # NEW: warning line (red when over end time)
        self.lbl_warning = tk.Label(root, text="", bg="#000000", fg="#ff2b2b",
                                    font=("Segoe UI", 12, "bold"), justify="left")
        self.lbl_warning.pack(anchor="w", padx=60, pady=(0, 10))

        spacer = tk.Frame(root, bg="#000000", height=120)
        spacer.pack(fill="x")

        self.refresh_dashboard()

    def refresh_dashboard(self):
        now = datetime.now()
        self.lbl_dash_time.configure(text=now.strftime("%m/%d/%Y %H:%M"))

        # ---------------- base totals from production ----------------
        trays_completed_total = 0.0
        cases_completed_total = 0.0
        trays_completed_ossid = 0.0
        trays_completed_repak = 0.0
        cases_completed_ossid = 0.0
        cases_completed_repak = 0.0

        if self.last_production_df is not None and not self.last_production_df.empty:
            p = self.last_production_df.copy()
            trays_completed_total = float(p["TraysProduced"].sum())
            cases_completed_total = float(p["CasesProduced"].sum())

            po = p[p["Machine"].astype(str).str.strip() == MACHINE_OSSID]
            pr = p[p["Machine"].astype(str).str.strip() == MACHINE_REPAK]

            trays_completed_ossid = float(po["TraysProduced"].sum()) if not po.empty else 0.0
            trays_completed_repak = float(pr["TraysProduced"].sum()) if not pr.empty else 0.0
            cases_completed_ossid = float(po["CasesProduced"].sum()) if not po.empty else 0.0
            cases_completed_repak = float(pr["CasesProduced"].sum()) if not pr.empty else 0.0

        # ---------------- remaining from shortsheet ----------------
        trays_remaining_total = 0.0
        cases_remaining_total = 0.0
        trays_remaining_ossid = 0.0
        trays_remaining_repak = 0.0
        cases_remaining_ossid = 0.0
        cases_remaining_repak = 0.0

        # also keep a per-PLU remaining frame for STANDARD estimates
        remain_detail = None

        if (self.last_shortsheets_df is not None and not self.last_shortsheets_df.empty and
                self.master_df is not None and not self.master_df.empty):
            ss = self.last_shortsheets_df.copy()
            m = self.master_df.copy()

            ss["PLU"] = ss["PLU"].astype(str).str.zfill(5)
            m["PLU"] = m["PLU"].astype(str).str.zfill(5)

            ss = ss.merge(m[["PLU", "Trays", "Machine", "TPM"]], on="PLU", how="left")
            ss["Trays"] = pd.to_numeric(ss["Trays"], errors="coerce").fillna(0).astype(float)
            ss["TPM"] = pd.to_numeric(ss["TPM"], errors="coerce").fillna(0).astype(float)
            ss["RemainingCases"] = pd.to_numeric(ss["RemainingCases"], errors="coerce").fillna(0).astype(float)
            ss["Machine"] = ss["Machine"].fillna("Unknown").astype(str).str.strip()

            ss["TraysRemaining"] = ss["RemainingCases"] * ss["Trays"]

            trays_remaining_total = float(ss["TraysRemaining"].sum())
            cases_remaining_total = float(ss["RemainingCases"].sum())

            so = ss[ss["Machine"] == MACHINE_OSSID]
            sr = ss[ss["Machine"] == MACHINE_REPAK]

            trays_remaining_ossid = float(so["TraysRemaining"].sum()) if not so.empty else 0.0
            trays_remaining_repak = float(sr["TraysRemaining"].sum()) if not sr.empty else 0.0
            cases_remaining_ossid = float(so["RemainingCases"].sum()) if not so.empty else 0.0
            cases_remaining_repak = float(sr["RemainingCases"].sum()) if not sr.empty else 0.0

            remain_detail = ss.copy()

        # ---------------- run minutes ----------------
        run_mins = 0.0
        start_dt = None
        end_dt = None
        try:
            st = parse_hhmm(self.var_start_time.get())
            et = parse_hhmm(self.var_end_time.get())
            start_dt = datetime.combine(now.date(), st)
            end_dt = _end_datetime_for_today(start_dt, et)
            run_mins = minutes_between(start_dt, now)
        except Exception:
            run_mins = 0.0

        # ---------------- ACTUAL rate overall + by machine ----------------
        trays_per_min = (trays_completed_total / run_mins) if run_mins > 0 else 0.0
        cases_per_min = (cases_completed_total / run_mins) if run_mins > 0 else 0.0

        trays_per_min_ossid = (trays_completed_ossid / run_mins) if run_mins > 0 else 0.0
        trays_per_min_repak = (trays_completed_repak / run_mins) if run_mins > 0 else 0.0

        # ---------------- set top KPI blocks ----------------
        self.kpi_traypack.set_value(f"{trays_completed_total:,.0f}" if trays_completed_total > 0 else "—")
        self.kpi_traysmin.set_value(f"{trays_per_min:,.2f}" if trays_per_min > 0 else "—")
        self.kpi_tpcases.set_value(f"{cases_completed_total:,.0f}" if cases_completed_total > 0 else "—")
        self.kpi_casesmin.set_value(f"{cases_per_min:,.2f}" if cases_per_min > 0 else "—")

        self.kpi_trays_to_complete.set_value(f"{trays_remaining_total:,.0f}" if trays_remaining_total > 0 else "—")
        self.kpi_cases_to_complete.set_value(f"{cases_remaining_total:,.0f}" if cases_remaining_total > 0 else "—")

        self.block_traysmin_by_machine.set_values(
            f"{trays_per_min_ossid:,.2f}" if trays_per_min_ossid > 0 else "—",
            f"{trays_per_min_repak:,.2f}" if trays_per_min_repak > 0 else "—",
        )

        # ratios already exist
        self.tray_completed.set_values(f"{trays_completed_ossid:,.0f}", f"{trays_completed_repak:,.0f}")
        self.tray_remaining.set_values(f"{trays_remaining_ossid:,.0f}", f"{trays_remaining_repak:,.0f}")
        self.case_completed.set_values(f"{cases_completed_ossid:,.0f}", f"{cases_completed_repak:,.0f}")
        self.case_remaining.set_values(f"{cases_remaining_ossid:,.0f}", f"{cases_remaining_repak:,.0f}")

        # ---------------- estimates text: ACTUAL + STANDARD by machine ----------------
        lines = []
        warn_lines = []

        # helper for a machine estimate
        def _fmt_actual(machine_name: str, remain_trays: float, rate: float):
            if remain_trays <= 0:
                return f"{machine_name} ACTUAL -> No remaining trays."
            if rate <= 0:
                return f"{machine_name} ACTUAL -> Need production/run minutes to calculate rate."
            mins = remain_trays / rate
            finish = now + timedelta(minutes=mins)
            return f"{machine_name} ACTUAL -> {rate:,.2f} trays/min | Est: {mins/60:,.2f} hrs | Finish: {finish.strftime('%H:%M')}"

        lines.append(_fmt_actual("OSSID", trays_remaining_ossid, trays_per_min_ossid))
        lines.append(_fmt_actual("REPAK", trays_remaining_repak, trays_per_min_repak))

        # overall actual (optional but useful)
        if trays_remaining_total > 0 and trays_per_min > 0:
            mins = trays_remaining_total / trays_per_min
            finish = now + timedelta(minutes=mins)
            lines.append(f"TOTAL ACTUAL -> {trays_per_min:,.2f} trays/min | Est: {mins/60:,.2f} hrs | Finish: {finish.strftime('%H:%M')}")
        else:
            lines.append("TOTAL ACTUAL -> (Run Production + Shortsheet)")

        # STANDARD (per-PLU sum of remaining_trays / StdTPM)
        def _standard_minutes_for_machine(df: pd.DataFrame, machine_code: str) -> float:
            d = df[df["Machine"].astype(str).str.strip() == machine_code].copy()
            if d.empty:
                return 0.0
            # only PLUs with StdTPM > 0
            d["TPM"] = pd.to_numeric(d["TPM"], errors="coerce").fillna(0).astype(float)
            d["TraysRemaining"] = pd.to_numeric(d["TraysRemaining"], errors="coerce").fillna(0).astype(float)
            d = d[(d["TPM"] > 0) & (d["TraysRemaining"] > 0)].copy()
            if d.empty:
                return 0.0
            return float((d["TraysRemaining"] / d["TPM"]).sum())

        if remain_detail is not None:
            std_mins_o = _standard_minutes_for_machine(remain_detail, MACHINE_OSSID)
            std_mins_r = _standard_minutes_for_machine(remain_detail, MACHINE_REPAK)
            std_mins_total = std_mins_o + std_mins_r

            if std_mins_o > 0:
                finish_o = now + timedelta(minutes=std_mins_o)
                lines.append(f"OSSID STANDARD -> Est: {std_mins_o/60:,.2f} hrs | Finish: {finish_o.strftime('%H:%M')}")
            else:
                lines.append("OSSID STANDARD -> StdTPM missing/0 for remaining Ossid items.")

            if std_mins_r > 0:
                finish_r = now + timedelta(minutes=std_mins_r)
                lines.append(f"REPAK STANDARD -> Est: {std_mins_r/60:,.2f} hrs | Finish: {finish_r.strftime('%H:%M')}")
            else:
                lines.append("REPAK STANDARD -> StdTPM missing/0 for remaining Repak items.")

            if std_mins_total > 0:
                finish_t = now + timedelta(minutes=std_mins_total)
                lines.append(f"TOTAL STANDARD -> Est: {std_mins_total/60:,.2f} hrs | Finish: {finish_t.strftime('%H:%M')}")
            else:
                lines.append("TOTAL STANDARD -> (Need StdTPM + remaining trays)")
        else:
            lines.append("STANDARD -> (Run Shortsheet + load Product Info with StdTPM)")

        self.lbl_estimates.configure(text="\n".join(lines))

        # ---------------- warning vs end time ----------------
        self.lbl_warning.configure(text="")
        if start_dt is not None and end_dt is not None:
            # Actual projections (by machine)
            if trays_remaining_ossid > 0 and trays_per_min_ossid > 0:
                finish_o = now + timedelta(minutes=(trays_remaining_ossid / trays_per_min_ossid))
                if finish_o > end_dt:
                    warn_lines.append(f"⚠ OSSID projected finish {finish_o.strftime('%H:%M')} is after END TIME {end_dt.strftime('%H:%M')}")

            if trays_remaining_repak > 0 and trays_per_min_repak > 0:
                finish_r = now + timedelta(minutes=(trays_remaining_repak / trays_per_min_repak))
                if finish_r > end_dt:
                    warn_lines.append(f"⚠ REPAK projected finish {finish_r.strftime('%H:%M')} is after END TIME {end_dt.strftime('%H:%M')}")

            if trays_remaining_total > 0 and trays_per_min > 0:
                finish_t = now + timedelta(minutes=(trays_remaining_total / trays_per_min))
                if finish_t > end_dt:
                    warn_lines.append(f"⚠ TOTAL projected finish {finish_t.strftime('%H:%M')} is after END TIME {end_dt.strftime('%H:%M')}")

        if warn_lines:
            self.lbl_warning.configure(text="\n".join(warn_lines))
        else:
            self.lbl_warning.configure(text="")

    # ---------------- Shortsheet ----------------
    def on_run_shortsheets(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        self._load_master(initial=True)

        try:
            txnd = self._parse_txndate()
        except ValueError:
            messagebox.showerror("TxnDate", "Enter TxnDate as YYYY-MM-DD.")
            return

        statuses = self._get_short_wip_statuses()
        only_remaining = bool(self.var_only_remaining.get())
        exclude_missing = bool(self.var_exclude_missing.get())
        exclude_frozen = bool(self.var_exclude_frozen.get())

        try:
            self._log("Connecting to SQL Server (shortsheet)...")
            with pyodbc.connect(build_connection_string(self.cfg), timeout=25) as conn:
                df = fetch_shortsheets(conn, txnd, statuses, only_remaining)

            if df.empty:
                self._log("Shortsheet returned 0 rows.")
                self.last_shortsheets_df = df
                self.short_table.set_dataframe(pd.DataFrame(columns=self.short_table.columns))
                self.refresh_dashboard()
                messagebox.showinfo("Shortsheet", "No remaining balances found.")
                return

            df2 = df.copy()

            if self.master_df is not None and not self.master_df.empty:
                m = self.master_df.copy()
                df2["PLU"] = df2["PLU"].astype(str).str.zfill(5)
                df2 = df2.merge(m[["PLU", "DESC", "Trays", "TPM", "Type", "Machine", "Frozen"]], on="PLU", how="left")
                df2 = df2.rename(columns={"DESC": "ProductDescription", "Trays": "TraysPerCase", "TPM": "StdTPM"})
            else:
                df2["ProductDescription"] = ""
                df2["TraysPerCase"] = None
                df2["Frozen"] = ""

            if exclude_missing and self.master_df is not None and not self.master_df.empty:
                df2["TraysPerCase"] = pd.to_numeric(df2["TraysPerCase"], errors="coerce")
                df2 = df2[df2["TraysPerCase"].notna()].copy()

            if exclude_frozen:
                df2["Frozen"] = df2["Frozen"].astype(str).str.strip().str.upper()
                df2 = df2[df2["Frozen"] != "Y"].copy()

            show_cols = ["ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"]
            for c in show_cols:
                if c not in df2.columns:
                    df2[c] = ""
            df2 = df2[show_cols]

            self.last_shortsheets_df = df2
            self.short_table.set_dataframe(df2)
            self._log(f"Shortsheet rows: {len(df2):,}")
            self.refresh_dashboard()
        except Exception as e:
            self._log(f"ERROR shortsheet: {e}")
            self._log(traceback.format_exc())
            messagebox.showerror("Shortsheet Error", str(e))

    def on_export_shortsheets(self):
        if self.last_shortsheets_df is None or self.last_shortsheets_df.empty:
            messagebox.showinfo("Export", "Run Shortsheet first.")
            return

        out_folder = (self.cfg.output_folder or "").strip() or os.getcwd()
        os.makedirs(out_folder, exist_ok=True)

        try:
            txnd = self._parse_txndate()
        except ValueError:
            txnd = date.today()

        out_path = os.path.join(out_folder, f"Shortsheet_TxnDate_{txnd.isoformat()}.xlsx")
        try:
            export_to_excel(self.last_shortsheets_df, out_path, "Shortsheet")
            self._log(f"Exported shortsheet: {out_path}")
            messagebox.showinfo("Export", f"Saved:\n{out_path}")
        except Exception as e:
            self._log(f"ERROR exporting shortsheet: {e}")
            messagebox.showerror("Export Error", str(e))

    # ---------------- Production ----------------
    def on_refresh_production(self):
        if pyodbc is None:
            messagebox.showerror("Missing Dependency", "pyodbc is not installed.")
            return

        self._load_master(initial=True)
        if self.master_df is None or self.master_df.empty:
            messagebox.showerror("Product Master", "Load Product Info.xlsx first (Settings).")
            return

        pack = (self.var_packdate.get() or "").strip()
        if not pack:
            messagebox.showerror("PackDate", "Enter the 4-digit packdate code (e.g., 1307).")
            return

        pack_param = int(pack) if pack.isdigit() else pack
        machine_filter = (self.var_machine.get() or "All").strip()
        exclude_missing = bool(self.var_exclude_missing.get())
        exclude_frozen = bool(self.var_exclude_frozen.get())

        try:
            parse_hhmm(self.var_start_time.get())
            parse_hhmm(self.var_end_time.get())
        except Exception:
            messagebox.showerror("Time", "Enter Start/End time as HH:MM (24-hour), e.g., 08:30")
            return

        try:
            self._log("Connecting to SQL Server (production)...")
            with pyodbc.connect(build_connection_string(self.cfg), timeout=25) as conn:
                prod_df = fetch_production_by_packdate(conn, pack_param, self.production_statuses)

            if prod_df.empty:
                self._log("Production query returned 0 rows.")
                self.last_production_df = pd.DataFrame()
                self.prod_table.set_dataframe(pd.DataFrame(columns=self.prod_table.columns))
                self.primal_table.set_dataframe(pd.DataFrame(columns=self.primal_table.columns))
                self.refresh_dashboard()
                messagebox.showinfo("Production", "No production found for that packdate/status set.")
                return

            m = self.master_df.copy()
            prod_df["PLU"] = prod_df["PLU"].astype(str).str.zfill(5)
            merged = prod_df.merge(m[["PLU", "DESC", "Trays", "TPM", "Type", "Machine", "Frozen"]], on="PLU", how="left")

            merged = merged.rename(columns={"Trays": "TraysPerCase", "TPM": "StdTPM"})
            merged["TraysPerCase"] = pd.to_numeric(merged["TraysPerCase"], errors="coerce")
            merged["StdTPM"] = pd.to_numeric(merged["StdTPM"], errors="coerce").fillna(0).astype(float)
            merged["CasesProduced"] = pd.to_numeric(merged["CasesProduced"], errors="coerce").fillna(0).astype(float)
            merged["LbsProduced"] = pd.to_numeric(merged["LbsProduced"], errors="coerce").fillna(0).astype(float)

            if exclude_missing:
                merged = merged[merged["TraysPerCase"].notna()].copy()

            if exclude_frozen:
                merged["Frozen"] = merged["Frozen"].astype(str).str.strip().str.upper()
                merged = merged[merged["Frozen"] != "Y"].copy()

            merged["Machine"] = merged["Machine"].fillna("Unknown").astype(str).str.strip()
            merged["Type"] = merged["Type"].fillna("Unknown")
            merged["DESC"] = merged["DESC"].fillna("")
            merged["TraysPerCase"] = merged["TraysPerCase"].fillna(0).astype(float)

            if machine_filter != "All":
                merged = merged[merged["Machine"].astype(str).str.strip() == machine_filter].copy()

            merged["TraysProduced"] = merged["CasesProduced"] * merged["TraysPerCase"]
            self.last_production_df = merged.copy()

            total_cases = float(merged["CasesProduced"].sum())
            total_lbs = float(merged["LbsProduced"].sum())
            total_trays = float(merged["TraysProduced"].sum())

            merged_display = merged[[
                "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
                "CasesProduced", "LbsProduced", "TraysProduced"
            ]].copy().sort_values(by=["Machine", "Type", "TraysProduced"], ascending=[True, True, False])

            primal = merged.groupby(["Machine", "Type"], as_index=False).agg(
                CasesProduced=("CasesProduced", "sum"),
                LbsProduced=("LbsProduced", "sum"),
                TraysProduced=("TraysProduced", "sum"),
            ).sort_values(by=["Machine", "TraysProduced"], ascending=[True, False])

            self.prod_summary.delete("1.0", "end")
            self.prod_summary.insert(
                "end",
                f"PackDate: {pack} | Machine filter: {machine_filter}\n"
                f"Totals -> Cases: {total_cases:,.0f} | Lbs: {total_lbs:,.1f} | Trays: {total_trays:,.0f}\n"
                f"Statuses: {', '.join(self.production_statuses)}\n"
                f"Exclude Frozen=Y: {exclude_frozen} | Exclude Missing Master: {exclude_missing}\n"
            )

            self.prod_table.set_dataframe(merged_display)
            self.primal_table.set_dataframe(primal)

            self._log(f"Production rows (PLU): {len(merged_display):,}")
            self.refresh_dashboard()
        except Exception as e:
            self._log(f"ERROR production: {e}")
            self._log(traceback.format_exc())
            messagebox.showerror("Production Error", str(e))

    def on_export_production(self):
        if self.last_production_df is None or self.last_production_df.empty:
            messagebox.showinfo("Export", "Refresh Production first.")
            return

        out_folder = (self.cfg.output_folder or "").strip() or os.getcwd()
        os.makedirs(out_folder, exist_ok=True)

        pack = (self.var_packdate.get() or "").strip() or "PackDate"
        machine_filter = (self.var_machine.get() or "All").strip()

        out_path = os.path.join(out_folder, f"Production_PackDate_{pack}_{machine_filter}.xlsx")
        try:
            merged = self.last_production_df.copy()
            by_plu = merged[[
                "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
                "CasesProduced", "LbsProduced", "TraysProduced"
            ]].copy().sort_values(by=["Machine", "Type", "TraysProduced"], ascending=[True, True, False])

            by_type = merged.groupby(["Machine", "Type"], as_index=False).agg(
                CasesProduced=("CasesProduced", "sum"),
                LbsProduced=("LbsProduced", "sum"),
                TraysProduced=("TraysProduced", "sum"),
            ).sort_values(by=["Machine", "TraysProduced"], ascending=[True, False])

            export_multi_to_excel({"ProductionByPLU": by_plu, "ProductionByType": by_type}, out_path)
            self._log(f"Exported production: {out_path}")
            messagebox.showinfo("Export", f"Saved:\n{out_path}")
        except Exception as e:
            self._log(f"ERROR exporting production: {e}")
            messagebox.showerror("Export Error", str(e))

    # ---------------- Tabs ----------------
    def _build_tab_shortsheets(self):
        pad = {"padx": 10, "pady": 6}

        frm = ttk.LabelFrame(self.tab_short, text="Inputs")
        frm.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm, text="TxnDate (YYYY-MM-DD):").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_txndate, width=16).grid(row=r, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm, text="Only Remaining (>0)", variable=self.var_only_remaining).grid(row=r, column=2, sticky="w", **pad)

        r += 1
        ttk.Label(frm, text="WIP statuses counted as AvailableCases:").grid(row=r, column=0, sticky="w", **pad)
        ttk.Checkbutton(frm, text="Available", variable=self.var_wip_available).grid(row=r, column=1, sticky="w", **pad)
        ttk.Checkbutton(frm, text="ScanningSalesOrder", variable=self.var_wip_scanning).grid(row=r, column=2, sticky="w", **pad)
        ttk.Checkbutton(frm, text="WaitingToBeInvoiced", variable=self.var_wip_waiting).grid(row=r, column=3, sticky="w", **pad)

        r += 1
        ttk.Button(frm, text="Run Shortsheet", command=self.on_run_shortsheets).grid(row=r, column=0, sticky="w", **pad)
        ttk.Button(frm, text="Export Shortsheet Excel", command=self.on_export_shortsheets).grid(row=r, column=1, sticky="w", **pad)

        self.short_table = TableView(self.tab_short, columns=[
            "ProductNumber", "PLU", "ProductDescription", "QtyOrdered", "QtyShipped", "AvailableCases", "RemainingCases"
        ], height=14)
        self.short_table.pack(fill="both", expand=True, padx=10, pady=10)

    def _build_tab_production(self):
        pad = {"padx": 10, "pady": 6}

        frm = ttk.LabelFrame(self.tab_prod, text="Inputs")
        frm.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm, text="PackDate (4-digit code):").grid(row=r, column=0, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_packdate, width=10).grid(row=r, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Machine:").grid(row=r, column=2, sticky="w", **pad)
        self.cmb_machine = ttk.Combobox(frm, textvariable=self.var_machine, values=["All"], width=10, state="readonly")
        self.cmb_machine.grid(row=r, column=3, sticky="w", **pad)

        ttk.Label(frm, text="Start (HH:MM):").grid(row=r, column=4, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_start_time, width=8).grid(row=r, column=5, sticky="w", **pad)

        ttk.Label(frm, text="End (HH:MM):").grid(row=r, column=6, sticky="w", **pad)
        ttk.Entry(frm, textvariable=self.var_end_time, width=8).grid(row=r, column=7, sticky="w", **pad)

        r += 1
        ttk.Button(frm, text="Refresh Production", command=self.on_refresh_production).grid(row=r, column=0, sticky="w", **pad)
        ttk.Button(frm, text="Export Production Excel", command=self.on_export_production).grid(row=r, column=1, sticky="w", **pad)

        self.prod_summary = tk.Text(self.tab_prod, height=8, wrap="word")
        self.prod_summary.pack(fill="x", padx=10, pady=10)

        self.prod_table = TableView(self.tab_prod, columns=[
            "Machine", "Type", "ProductNumber", "PLU", "DESC", "TraysPerCase", "StdTPM",
            "CasesProduced", "LbsProduced", "TraysProduced"
        ], height=10)
        ttk.Label(self.tab_prod, text="Production by PLU").pack(anchor="w", padx=10)
        self.prod_table.pack(fill="both", expand=True, padx=10, pady=6)

        self.primal_table = TableView(self.tab_prod, columns=[
            "Machine", "Type", "CasesProduced", "LbsProduced", "TraysProduced"
        ], height=8)
        ttk.Label(self.tab_prod, text="Production by Primal (Type)").pack(anchor="w", padx=10)
        self.primal_table.pack(fill="both", expand=True, padx=10, pady=6)


def main():
    App().mainloop()


if __name__ == "__main__":
    main()
