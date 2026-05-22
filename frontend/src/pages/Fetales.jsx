import { useEffect } from "react";
import { useRouter } from "next/router";

// Redirección para mantener compatibilidad si había enlaces antiguos
export default function Fetales() {
  const router = useRouter();
  useEffect(() => {
    router.replace("/dashboard/mortalidad");
  }, [router]);
  return null;
}
