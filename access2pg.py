import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from PIL import Image, ImageTk
import migrator
import os

class AccessToPostgresApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Migrador Access → PostgreSQL")
        self.root.geometry("850x700")
        self.root.resizable(False, False)
        self.cancel_requested = False
        self.show_splash()

    # ===== Splash =====
    def show_splash(self):
        splash = tk.Toplevel(self.root)
        splash.overrideredirect(True)
        splash.geometry("700x450+400+150")
        splash.configure(bg="#f0f0f0")

        try:
            img_path = os.path.join(os.path.dirname(__file__), "splash.png")
            img = Image.open(img_path)
            img = img.resize((680, 350), Image.LANCZOS)
            splash_img = ImageTk.PhotoImage(img)
            label_img = tk.Label(splash, image=splash_img, bg="#f0f0f0")
            label_img.image = splash_img
            label_img.pack(pady=20)
        except:
            label_txt = tk.Label(splash, text="Migrando bases de datos\nAccess → PostgreSQL",
                                 font=("Segoe UI", 18, "bold"), bg="#f0f0f0", fg="#333")
            label_txt.pack(expand=True)

        self.loading_label = tk.Label(splash, text="Inicializando...", bg="#f0f0f0", fg="#555", font=("Segoe UI", 11))
        self.loading_label.pack(side="bottom", pady=15)
        self.animate_loading(splash)

        self.root.withdraw()
        self.root.after(3000, lambda: (splash.destroy(), self.show_main_window()))

    def animate_loading(self, splash):
        dots = [".", "..", "..."]
        def cycle(i=0):
            if not splash.winfo_exists():
                return
            self.loading_label.config(text=f"Inicializando{dots[i % len(dots)]}")
            splash.after(500, cycle, i + 1)
        cycle()

    # ===== Ventana principal =====
    def show_main_window(self):
        self.root.deiconify()
        main_frame = ttk.Frame(self.root, padding=15)
        main_frame.pack(fill="both", expand=True)

        ttk.Label(main_frame, text="Configuración de Migración Access → PostgreSQL",
                  font=("Segoe UI", 14, "bold")).grid(row=0, column=0, columnspan=3, pady=(0, 15))

        # Archivo Access
        ttk.Label(main_frame, text="Archivo Access:").grid(row=1, column=0, sticky="w", pady=3)
        self.access_path = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.access_path, width=70).grid(row=1, column=1, sticky="w")
        ttk.Button(main_frame, text="Examinar...", command=self.browse_access).grid(row=1, column=2, padx=5)

        # Host
        ttk.Label(main_frame, text="Host:").grid(row=2, column=0, sticky="w", pady=3)
        self.pg_host = tk.StringVar(value="localhost")
        ttk.Entry(main_frame, textvariable=self.pg_host, width=30).grid(row=2, column=1, sticky="w")

        # Puerto
        ttk.Label(main_frame, text="Puerto:").grid(row=2, column=2, sticky="e")
        self.pg_port = tk.StringVar(value="5432")
        ttk.Entry(main_frame, textvariable=self.pg_port, width=10).grid(row=2, column=2, sticky="w", padx=(80, 0))

        # Usuario
        ttk.Label(main_frame, text="Usuario:").grid(row=3, column=0, sticky="w", pady=3)
        self.pg_user = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.pg_user, width=30).grid(row=3, column=1, sticky="w")

        # Contraseña
        ttk.Label(main_frame, text="Contraseña:").grid(row=3, column=2, sticky="e")
        self.pg_pass = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.pg_pass, width=25, show="*").grid(row=3, column=2, sticky="w", padx=(80,0))

        # Base destino
        ttk.Label(main_frame, text="Base destino:").grid(row=4, column=0, sticky="w", pady=3)
        self.pg_db = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.pg_db, width=30).grid(row=4, column=1, sticky="w")

        # Esquema
        ttk.Label(main_frame, text="Esquema destino:").grid(row=4, column=2, sticky="e")
        self.pg_schema = tk.StringVar(value="public")
        ttk.Entry(main_frame, textvariable=self.pg_schema, width=25).grid(row=4, column=2, sticky="w", padx=(80,0))

        # Progreso
        ttk.Label(main_frame, text="Progreso:").grid(row=5, column=0, sticky="w", pady=(15,3))
        self.progress = ttk.Progressbar(main_frame, length=750, mode="determinate")
        self.progress.grid(row=6, column=0, columnspan=3, pady=5)

        # Log
        ttk.Label(main_frame, text="Registro de eventos:").grid(row=7, column=0, sticky="w", pady=(10,3))
        self.log = tk.Text(main_frame, height=16, wrap="word", font=("Consolas", 9))
        self.log.grid(row=8, column=0, columnspan=3, sticky="nsew", pady=(0,10))
        main_frame.grid_rowconfigure(8, weight=1)
        main_frame.grid_columnconfigure(1, weight=1)

        # Botones
        btn_frame = ttk.Frame(main_frame)
        btn_frame.grid(row=9, column=0, columnspan=3, pady=10)
        self.start_btn = ttk.Button(btn_frame, text="Iniciar migración", command=self.start_migration)
        self.start_btn.pack(side="left", padx=5)
        self.cancel_btn = ttk.Button(btn_frame, text="Cancelar", command=self.cancel_migration, state="disabled")
        self.cancel_btn.pack(side="left", padx=5)
        self.exit_btn = ttk.Button(btn_frame, text="Finalizar y salir", command=self.root.destroy)
        self.exit_btn.pack(side="left", padx=5)

    # ===== Funciones =====
    def browse_access(self):
        path = filedialog.askopenfilename(filetypes=[("Access", "*.accdb *.mdb"), ("Todos", "*.*")])
        if path:
            self.access_path.set(path)

    def cancel_migration(self):
        self.cancel_requested = True
        self.log.insert("end", "Cancelando proceso...\n")
        self.log.see("end")

    def start_migration(self):
        if not self.access_path.get():
            messagebox.showwarning("Falta archivo", "Seleccione un archivo Access primero.")
            return
        if not self.pg_db.get():
            messagebox.showwarning("Falta base de datos", "Ingrese el nombre de la base destino.")
            return
        self.start_btn.config(state="disabled")
        self.cancel_btn.config(state="normal")
        self.progress["value"] = 0
        self.log.delete("1.0", "end")
        self.cancel_requested = False
        thread = threading.Thread(target=self.run_migration)
        thread.start()

    def run_migration(self):
        try:
            pg_params = {
                "host": self.pg_host.get(),
                "port": self.pg_port.get(),
                "user": self.pg_user.get(),
                "password": self.pg_pass.get(),
                "dbname": self.pg_db.get(),
            }

            def progress_callback(msg, percent):
                if self.cancel_requested:
                    raise Exception("Proceso cancelado por el usuario.")
                self.progress["value"] = percent
                self.log.insert("end", msg + "\n")
                self.log.see("end")

            migrator.migrate_access_to_postgres(
                self.access_path.get(),
                pg_params,
                self.pg_schema.get(),
                progress_callback
            )
            messagebox.showinfo("Completado", "Migración finalizada correctamente.")
        except Exception as e:
            messagebox.showerror("Error durante la migración", str(e))
        finally:
            self.start_btn.config(state="normal")
            self.cancel_btn.config(state="disabled")
            self.progress["value"] = 0


if __name__ == "__main__":
    root = tk.Tk()
    app = AccessToPostgresApp(root)
    root.mainloop()
