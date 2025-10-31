import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from PIL import Image, ImageTk
from migrator import migrate_access_to_postgres

# ---------- Splash ----------
def show_splash(next_callback):
    splash = tk.Toplevel()
    splash.overrideredirect(True)
    splash.geometry("420x320+500+250")
    splash.configure(bg="white")

    try:
        img = Image.open("assets/splash.png")
        img = img.resize((400, 280), Image.LANCZOS)
        photo = ImageTk.PhotoImage(img)
        lbl = tk.Label(splash, image=photo, bg="white")
        lbl.image = photo
        lbl.pack(pady=10)
    except Exception:
        tk.Label(splash, text="Access → PostgreSQL Migrator", font=("Segoe UI", 16, "bold"), bg="white").pack(pady=50)

    tk.Label(splash, text="Cargando...", font=("Segoe UI", 10), bg="white").pack()
    splash.after(2500, lambda: (splash.destroy(), next_callback()))

# ---------- Ventana principal ----------
def start_gui():
    root = tk.Tk()
    root.title("Access → PostgreSQL Migrator")
    root.geometry("420x480")

    # --- Widgets GUI ---
    tk.Label(root, text="Archivo Access (.accdb):").pack()
    access_entry = tk.Entry(root, width=50)
    access_entry.pack()

    def select_file():
        file_path = filedialog.askopenfilename(
            title="Seleccione archivo Access",
            filetypes=[("Access Database", "*.accdb *.mdb")]
        )
        access_entry.delete(0, tk.END)
        access_entry.insert(0, file_path)

    tk.Button(root, text="Examinar", command=select_file).pack()

    tk.Label(root, text="Host PostgreSQL:").pack()
    host_entry = tk.Entry(root)
    host_entry.insert(0, "localhost")
    host_entry.pack()

    tk.Label(root, text="Puerto:").pack()
    port_entry = tk.Entry(root)
    port_entry.insert(0, "5432")
    port_entry.pack()

    tk.Label(root, text="Base de datos destino:").pack()
    db_entry = tk.Entry(root)
    db_entry.pack()

    tk.Label(root, text="Esquema destino:").pack()
    schema_entry = tk.Entry(root)
    schema_entry.insert(0, "public")
    schema_entry.pack()

    tk.Label(root, text="Usuario:").pack()
    user_entry = tk.Entry(root)
    user_entry.pack()

    tk.Label(root, text="Contraseña:").pack()
    password_entry = tk.Entry(root, show="*")
    password_entry.pack()

    # --- Progreso ---
    def start_migration():
        access_path = access_entry.get()
        pg_params = {
            'host': host_entry.get(),
            'dbname': db_entry.get(),
            'user': user_entry.get(),
            'password': password_entry.get(),
            'port': port_entry.get()
        }
        schema = schema_entry.get()

        if not access_path:
            messagebox.showerror("Error", "Seleccione un archivo Access (.accdb)")
            return
        if not pg_params['dbname']:
            messagebox.showerror("Error", "Debe ingresar el nombre de la base de datos destino.")
            return
        if not schema:
            messagebox.showerror("Error", "Debe ingresar el esquema destino.")
            return

        progress_window = tk.Toplevel(root)
        progress_window.title("Progreso de migración")
        progress_window.geometry("400x150")

        status_label = tk.Label(progress_window, text="Preparando migración...")
        status_label.pack(pady=10)

        progress_bar = ttk.Progressbar(progress_window, orient="horizontal", length=300, mode="determinate")
        progress_bar.pack(pady=10)

        def update_progress(msg, percent):
            status_label.config(text=msg)
            progress_bar['value'] = percent
            progress_window.update_idletasks()

        def run_migration():
            try:
                migrate_access_to_postgres(access_path, pg_params, schema, progress_callback=update_progress)
                messagebox.showinfo("Éxito", f"Migración completada en esquema '{schema}'.")
            except Exception as e:
                messagebox.showerror("Error durante la migración", str(e))
            finally:
                progress_window.destroy()

        threading.Thread(target=run_migration, daemon=True).start()

    tk.Button(root, text="Iniciar Migración", command=start_migration, bg="green", fg="white").pack(pady=20)

    root.mainloop()

# ---------- Lanzador ----------
if __name__ == "__main__":
    main = tk.Tk()
    main.withdraw()
    show_splash(start_gui)
    main.mainloop()
