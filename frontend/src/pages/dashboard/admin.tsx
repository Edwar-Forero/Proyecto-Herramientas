import { FormEvent, useEffect, useState } from "react";
import { DashboardLayout } from "@/layouts/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { createUser, getSystemStatus, listUsers, uploadDataset } from "@/services/dashboard";
import { getErrorMessage } from "@/services/api";
import type { SystemStatus, User } from "@/types/api";
import { formatNumber } from "@/lib/utils";

export default function AdminPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const [newUser, setNewUser] = useState({ username: "", password: "", rol: "consulta" });
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [collection, setCollection] = useState("nacimientos");

  async function load() {
    setLoading(true);
    try {
      const [u, s] = await Promise.all([listUsers(), getSystemStatus()]);
      setUsers(u);
      setStatus(s);
    } catch (e) {
      setError(getErrorMessage(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  async function handleCreateUser(e: FormEvent) {
    e.preventDefault();
    setMessage("");
    try {
      await createUser(newUser);
      setMessage("Usuario creado correctamente");
      setNewUser({ username: "", password: "", rol: "consulta" });
      load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  async function handleUpload(e: FormEvent) {
    e.preventDefault();
    if (!uploadFile) return;
    setMessage("");
    try {
      const result = await uploadDataset(uploadFile, collection, false);
      setMessage(`Carga exitosa: ${result.inserted} registros en ${result.collection}`);
      load();
    } catch (err) {
      setError(getErrorMessage(err));
    }
  }

  return (
    <DashboardLayout
      title="Administración"
      subtitle="Usuarios, carga de datos y estado del sistema"
      allowedRoles={["admin"]}
    >
      {error && <p className="mb-4 text-sm text-rose-400">{error}</p>}
      {message && <p className="mb-4 text-sm text-emerald-400">{message}</p>}

      {loading ? (
        <Skeleton className="h-64 w-full" />
      ) : (
        <div className="grid gap-6 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Estado del sistema</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              {status && (
                <>
                  <p>
                    Base de datos: <span className="text-primary">{status.database}</span>
                  </p>
                  <p>Total registros: {formatNumber(status.total_records)}</p>
                  <ul className="mt-2 space-y-1 text-muted-foreground">
                    {Object.entries(status.collections).map(([name, count]) => (
                      <li key={name}>
                        {name}: {formatNumber(count)}
                      </li>
                    ))}
                  </ul>
                </>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Crear usuario</CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleCreateUser} className="space-y-3">
                <div>
                  <Label>Usuario</Label>
                  <Input
                    value={newUser.username}
                    onChange={(e) => setNewUser({ ...newUser, username: e.target.value })}
                    required
                  />
                </div>
                <div>
                  <Label>Contraseña</Label>
                  <Input
                    type="password"
                    value={newUser.password}
                    onChange={(e) => setNewUser({ ...newUser, password: e.target.value })}
                    required
                  />
                </div>
                <div>
                  <Label>Rol</Label>
                  <select
                    value={newUser.rol}
                    onChange={(e) => setNewUser({ ...newUser, rol: e.target.value })}
                    className="mt-1 h-10 w-full rounded-lg border border-border bg-muted/50 px-3 text-sm"
                  >
                    <option value="admin">Administrador</option>
                    <option value="analista">Analista</option>
                    <option value="consulta">Usuario</option>
                  </select>
                </div>
                <Button type="submit">Crear</Button>
              </form>
            </CardContent>
          </Card>

          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Usuarios ({users.length})</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-muted-foreground border-b border-border">
                      <th className="pb-2">Usuario</th>
                      <th className="pb-2">Rol</th>
                      <th className="pb-2">Estado</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.map((u) => (
                      <tr key={u.id} className="border-b border-border/50">
                        <td className="py-2">{u.username}</td>
                        <td className="py-2">{u.rol_label}</td>
                        <td className="py-2">{u.activo ? "Activo" : "Inactivo"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>

          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Cargar dataset (.parquet)</CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleUpload} className="flex flex-wrap gap-4 items-end">
                <div>
                  <Label>Archivo</Label>
                  <Input
                    type="file"
                    accept=".parquet"
                    onChange={(e) => setUploadFile(e.target.files?.[0] ?? null)}
                  />
                </div>
                <div>
                  <Label>Colección</Label>
                  <select
                    value={collection}
                    onChange={(e) => setCollection(e.target.value)}
                    className="mt-1 h-10 rounded-lg border border-border bg-muted/50 px-3 text-sm block"
                  >
                    <option value="nacimientos">Nacimientos</option>
                    <option value="defunciones_fetales">Defunciones fetales</option>
                    <option value="defunciones_no_fetales">Defunciones no fetales</option>
                  </select>
                </div>
                <Button type="submit" disabled={!uploadFile}>
                  Cargar
                </Button>
              </form>
            </CardContent>
          </Card>
        </div>
      )}
    </DashboardLayout>
  );
}
