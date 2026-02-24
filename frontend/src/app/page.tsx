"use client";

import { useState, useRef, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { KeyRound, Link as LinkIcon, UploadCloud, Rocket, CheckCircle2, AlertCircle, FileJson, Loader2 } from "lucide-react";

export default function Home() {
    const [apiKey, setApiKey] = useState("");
    const [url, setUrl] = useState("");
    const [gtmFile, setGtmFile] = useState<File | null>(null);
    const [status, setStatus] = useState<"idle" | "crawling" | "analyzing" | "review_required" | "compiling" | "success" | "error">("idle");
    const [taskId, setTaskId] = useState<string | null>(null);
    const [trackingPlan, setTrackingPlan] = useState<any[]>([]);
    const [logs, setLogs] = useState<string[]>([]);
    const [result, setResult] = useState<string | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        const savedKey = localStorage.getItem("geminiApiKey");
        const savedUrl = localStorage.getItem("targetUrl");
        if (savedKey) setApiKey(savedKey);
        if (savedUrl) setUrl(savedUrl);
    }, []);

    useEffect(() => {
        if (apiKey) localStorage.setItem("geminiApiKey", apiKey);
    }, [apiKey]);

    useEffect(() => {
        if (url) localStorage.setItem("targetUrl", url);
    }, [url]);

    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (e.target.files && e.target.files.length > 0) {
            setGtmFile(e.target.files[0]);
        }
    };

    const addLog = (message: string) => {
        setLogs((prev) => [...prev, message]);
    };

    // Polling Logic
    useEffect(() => {
        let interval: NodeJS.Timeout;

        const pollStatus = async () => {
            if (!taskId || status === "review_required" || status === "success" || status === "error") return;

            try {
                const res = await fetch(`http://localhost:8000/api/status/${taskId}`);
                if (!res.ok) throw new Error("Failed to poll status");
                const data = await res.json();

                if (data.status === "crawling" && status !== "crawling") {
                    setStatus("crawling");
                }

                if (data.status === "analyzing" && status !== "analyzing") {
                    setStatus("analyzing");
                }

                if (data.logs && Array.isArray(data.logs)) {
                    setLogs(data.logs);
                }

                if (data.status === "review_required") {
                    setStatus("review_required");
                    setTrackingPlan(data.tracking_plan?.tracking_plan || []);
                    addLog("⚠️ Tracking Plan proposed! Please review before injection.");
                    clearInterval(interval);
                }

                if (data.status === "error") {
                    throw new Error(data.error || "Unknown processing error");
                }

            } catch (err: any) {
                addLog(`❌ Polling Error: ${err.message}`);
                setStatus("error");
                clearInterval(interval);
            }
        };

        if (taskId) {
            interval = setInterval(pollStatus, 2000);
        }

        return () => clearInterval(interval);
    }, [taskId, status]);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!apiKey || !url || !gtmFile) return;

        setStatus("crawling");
        setLogs(["🚀 Initializing AutoGTM Task Queue..."]);

        const formData = new FormData();
        formData.append("target_url", url);
        formData.append("gemini_key", apiKey);
        formData.append("gtm_file", gtmFile);

        try {
            const res = await fetch("http://localhost:8000/api/analyze", {
                method: "POST",
                body: formData,
            });

            if (!res.ok) {
                const errorData = await res.json();
                throw new Error(errorData.detail || "Server request failed");
            }

            const data = await res.json();
            setTaskId(data.task_id);
            addLog(`✅ Task Queued in Background (ID: ${data.task_id.substring(0, 8)}...)`);

        } catch (error: any) {
            addLog(`❌ Error: ${error.message}`);
            setStatus("error");
        }
    };

    const handleCompile = async () => {
        if (!taskId) return;
        setStatus("compiling");
        addLog("⚙️ Compiling approved tags into GTM JSON...");

        try {
            const res = await fetch(`http://localhost:8000/api/compile/${taskId}`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    approved_plan: { tracking_plan: trackingPlan }
                })
            });

            if (!res.ok) throw new Error("Compilation failed");

            const data = await res.json();
            if (!data.modified_gtm) {
                throw new Error("Backend returned empty GTM container. Please try again.");
            }
            const compiled = JSON.stringify(data.modified_gtm, null, 2);
            setResult(compiled);
            setStatus("success");
            addLog("🎉 GTM Injection Complete. Download your container below.");

        } catch (error: any) {
            addLog(`❌ Error: ${error.message}`);
            setStatus("error");
        }
    };

    const togglePlanItem = (index: number) => {
        const newPlan = [...trackingPlan];
        // simple deletion for now, could add toggle property later
        newPlan.splice(index, 1);
        setTrackingPlan(newPlan);
        addLog(`✂️ Removed tag suggestion #${index + 1}`);
    };

    const downloadJson = () => {
        if (!result) {
            console.error('downloadJson: result is empty');
            return;
        }
        try {
            // Validate it is real JSON before downloading
            JSON.parse(result);
        } catch (e) {
            console.error('downloadJson: result is not valid JSON:', result.substring(0, 100));
            return;
        }
        // data: URL approach — same-origin, no CORS, no navigation away from page
        const dataUrl = 'data:application/json;charset=utf-8,' + encodeURIComponent(result);
        const a = document.createElement('a');
        a.href = dataUrl;
        try {
            const domain = new URL(url).hostname.replace("www.", "");
            a.download = `AutoGTM_Enhanced_Container_${domain}.json`;
        } catch (e) {
            a.download = 'AutoGTM_Enhanced_Container.json';
        }
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    };

    return (
        <main className="min-h-screen bg-slate-950 text-slate-100 selection:bg-indigo-500/30 font-sans flex flex-col items-center py-12 px-4 sm:px-6 lg:px-8">
            {/* Background gradients */}
            <div className="fixed inset-0 overflow-hidden pointer-events-none -z-10 bg-slate-950">
                <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] rounded-full bg-indigo-600/20 blur-[120px]" />
                <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] rounded-full bg-rose-600/20 blur-[120px]" />
            </div>

            <div className="w-full max-w-5xl">
                <header className="mb-12 text-center md:text-left">
                    <motion.div initial={{ opacity: 0, y: -20 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.5 }}>
                        <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-indigo-400 to-rose-400">
                            AutoGTM Builder
                        </h1>
                        <p className="mt-3 text-lg text-slate-400 max-w-2xl">
                            Automate GA4 tracking plans with Playwright and Gemini. Upload your container, point to a URL, and let AI do the heavy lifting.
                        </p>
                    </motion.div>
                </header>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                    {/* Controls Panel */}
                    <motion.div
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ duration: 0.5, delay: 0.1 }}
                        className="flex flex-col gap-6"
                    >
                        <form onSubmit={handleSubmit} className="flex flex-col gap-6 bg-slate-900/50 backdrop-blur-xl border border-slate-800 rounded-2xl p-6 shadow-2xl">

                            {/* API Key */}
                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Gemini API Key</label>
                                <div className="relative">
                                    <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                                        <KeyRound className="h-5 w-5 text-slate-500" />
                                    </div>
                                    <input
                                        type="text"
                                        value={apiKey}
                                        onChange={(e) => setApiKey(e.target.value)}
                                        required
                                        placeholder="AIzaSy..."
                                        className="block w-full pl-10 pr-3 py-3 border border-slate-700 rounded-xl leading-5 bg-slate-950 text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm transition-all"
                                    />
                                </div>
                            </div>

                            {/* URL */}
                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Target URL</label>
                                <div className="relative">
                                    <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                                        <LinkIcon className="h-5 w-5 text-slate-500" />
                                    </div>
                                    <input
                                        type="url"
                                        value={url}
                                        onChange={(e) => setUrl(e.target.value)}
                                        required
                                        placeholder="https://example.com"
                                        className="block w-full pl-10 pr-3 py-3 border border-slate-700 rounded-xl leading-5 bg-slate-950 text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm transition-all"
                                    />
                                </div>
                            </div>

                            {/* File Upload */}
                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Base GTM Container (JSON)</label>
                                <div
                                    className={`mt-1 flex justify-center px-6 pt-5 pb-6 border-2 border-dashed rounded-xl transition-all cursor-pointer ${gtmFile ? 'border-indigo-500 bg-indigo-500/10' : 'border-slate-700 hover:border-slate-500 bg-slate-950'
                                        }`}
                                    onClick={() => fileInputRef.current?.click()}
                                >
                                    <div className="space-y-1 text-center">
                                        {gtmFile ? (
                                            <FileJson className="mx-auto h-12 w-12 text-indigo-400" />
                                        ) : (
                                            <UploadCloud className="mx-auto h-12 w-12 text-slate-500" />
                                        )}
                                        <div className="flex text-sm text-slate-400 justify-center">
                                            <span className="relative cursor-pointer rounded-md font-medium text-indigo-400 focus-within:outline-none hover:text-indigo-300">
                                                {gtmFile ? gtmFile.name : "Upload a JSON file"}
                                            </span>
                                        </div>
                                        <p className="text-xs text-slate-500">Max 10MB</p>
                                    </div>
                                </div>
                                <input
                                    type="file"
                                    accept=".json"
                                    className="hidden"
                                    ref={fileInputRef}
                                    onChange={handleFileChange}
                                    required
                                />
                            </div>

                            <button
                                type="submit"
                                disabled={status !== "idle" && status !== "error"}
                                className="mt-4 w-full flex justify-center items-center py-3 px-4 border border-transparent rounded-xl shadow-sm text-sm font-semibold text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 focus:ring-offset-slate-900 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
                            >
                                {(status === "crawling" || status === "analyzing" || status === "compiling") ? (
                                    <>
                                        <Loader2 className="animate-spin -ml-1 mr-2 h-5 w-5" />
                                        {status === "crawling" ? "Crawling Site..." : status === "analyzing" ? "AI Analysis..." : "Compiling..."}
                                    </>
                                ) : (
                                    <>
                                        <Rocket className="-ml-1 mr-2 h-5 w-5" />
                                        Generate Tracking Plan
                                    </>
                                )}
                            </button>
                        </form>
                    </motion.div>

                    {/* Logs & Result Panel */}
                    <motion.div
                        initial={{ opacity: 0, x: 20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ duration: 0.5, delay: 0.2 }}
                        className="flex flex-col h-full space-y-4"
                    >
                        <div className="flex-1 bg-slate-900/50 backdrop-blur-xl border border-slate-800 rounded-2xl p-6 shadow-2xl flex flex-col min-h-[400px]">
                            <h2 className="text-lg font-semibold text-slate-200 mb-4 flex items-center">
                                Execution Logs
                            </h2>

                            {/* Visual Tracking Plan Review */}
                            {status === "review_required" && (
                                <motion.div
                                    initial={{ opacity: 0, height: 0 }}
                                    animate={{ opacity: 1, height: "auto" }}
                                    className="pt-4 border-t border-slate-800"
                                >
                                    <h3 className="text-md font-medium text-amber-400 mb-3 flex items-center">
                                        <AlertCircle className="w-5 h-5 mr-2" />
                                        Review proposed tags
                                    </h3>
                                    <div className="space-y-3 mb-6">
                                        {trackingPlan.length === 0 ? (
                                            <p className="text-slate-400 text-sm">No trackable conversion events found on this page.</p>
                                        ) : (
                                            trackingPlan.map((plan, i) => (
                                                <div key={i} className="flex justify-between items-center bg-slate-800/50 p-3 rounded-lg border border-slate-700">
                                                    <div>
                                                        <div className="font-medium text-slate-200">{plan.event_name}</div>
                                                        <div className="text-xs text-slate-400 mt-1">
                                                            Trigger: <span className="text-indigo-300">{plan.trigger_type}</span>
                                                            {plan.trigger_condition && ` (${plan.trigger_condition.key}: ${plan.trigger_condition.value})`}
                                                        </div>
                                                    </div>
                                                    <button
                                                        onClick={() => togglePlanItem(i)}
                                                        className="text-slate-500 hover:text-rose-400 transition-colors p-2"
                                                        title="Remove tag"
                                                    >
                                                        🗑️
                                                    </button>
                                                </div>
                                            ))
                                        )}
                                    </div>
                                    <button
                                        onClick={handleCompile}
                                        className="w-full inline-flex justify-center items-center px-4 py-3 border border-transparent shadow-sm text-sm font-semibold rounded-xl text-white bg-indigo-600 hover:bg-indigo-500 transition-all"
                                    >
                                        <CheckCircle2 className="-ml-1 mr-2 h-5 w-5" />
                                        Approve & Compile GTM JSON
                                    </button>
                                </motion.div>
                            )}

                            <div className="flex-1 overflow-y-auto space-y-3 font-mono text-sm pr-2 text-slate-300 custom-scrollbar mt-4">
                                <AnimatePresence>
                                    {logs.length === 0 && status === "idle" && (
                                        <motion.p initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-slate-500 text-center mt-10">
                                            Awaiting input...
                                        </motion.p>
                                    )}
                                    {logs.map((log, i) => (
                                        <motion.div
                                            key={i}
                                            initial={{ opacity: 0, x: -10 }}
                                            animate={{ opacity: 1, x: 0 }}
                                            className="py-1 border-l-2 pl-3 border-indigo-500/50 bg-slate-800/30 rounded-r-md"
                                        >
                                            {log}
                                        </motion.div>
                                    ))}
                                </AnimatePresence>
                            </div>

                            {/* Success Actions */}
                            <AnimatePresence>
                                {status === "success" && (
                                    <motion.div
                                        initial={{ opacity: 0, y: 20 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        className="mt-6 pt-6 border-t border-slate-800 flex flex-col items-center"
                                    >
                                        <CheckCircle2 className="h-12 w-12 text-emerald-400 mb-3" />
                                        <button
                                            onClick={downloadJson}
                                            className="w-full inline-flex justify-center items-center px-4 py-3 border border-transparent shadow-sm text-sm font-semibold rounded-xl text-emerald-950 bg-emerald-400 hover:bg-emerald-300 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-emerald-500 focus:ring-offset-slate-900 transition-all"
                                        >
                                            <FileJson className="-ml-1 mr-2 h-5 w-5" />
                                            Download Enhanced GTM Container
                                        </button>
                                    </motion.div>
                                )}

                                {status === "error" && (
                                    <motion.div
                                        initial={{ opacity: 0, y: 20 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        className="mt-6 pt-6 border-t border-slate-800 flex flex-col items-center"
                                    >
                                        <AlertCircle className="h-12 w-12 text-rose-400 mb-3" />
                                        <p className="text-rose-400 font-medium">An error occurred during processing.</p>
                                    </motion.div>
                                )}
                            </AnimatePresence>
                        </div>
                    </motion.div>
                </div>
            </div>

            {/* Global styles for custom scrollbar hidden in Tailwind base */}
            <style dangerouslySetInnerHTML={{
                __html: `
        .custom-scrollbar::-webkit-scrollbar {
          width: 6px;
        }
        .custom-scrollbar::-webkit-scrollbar-track {
          background: transparent;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb {
          background-color: #334155;
          border-radius: 20px;
        }
      `}} />
        </main>
    );
}
