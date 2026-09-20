import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { integrationApiUrl } from "./api/config";

function StartupPage() {
  const [status, setStatus] = useState("checking");
  useEffect(() => { fetch(`${integrationApiUrl}/health/live`).then((r) => setStatus(r.ok ? "connected" : "unavailable")).catch(() => setStatus("unavailable")); }, []);
  return <main><h1>Integration Platform Admin</h1><p>Milestone 0 placeholder.</p><p>Integration API: {status}</p></main>;
}

createRoot(document.getElementById("root")!).render(<StrictMode><BrowserRouter><Routes><Route path="*" element={<StartupPage />} /></Routes></BrowserRouter></StrictMode>);

