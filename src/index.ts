import dotenv from "dotenv";
dotenv.config();

import express from "express";
import cors from "cors";
import multer from "multer";
import bodyParser from "body-parser";
import { randomUUID } from "crypto";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

/* ================== ENV ================== */

const PORT_PANEL = parseInt(process.env.PANEL_PORT ?? "3001", 10);
// главный порт: сначала PORT (Render/VPS), потом PANEL_PORT (локалка)
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : PORT_PANEL;

const BOT_API_BASE = process.env.BOT_API_BASE || "http://localhost:3002";
const ADMIN_PIN = process.env.PUSHLINE_ADMIN_PIN || "1234";

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY || "";
const OPENROUTER_MODEL =
  process.env.OPENROUTER_MODEL || "meta-llama/Meta-Llama-3.1-8B-Instruct:free";
const OPENROUTER_API_BASE =
  process.env.OPENROUTER_API_BASE || "https://openrouter.ai/api/v1/chat/completions";

console.log("OPENROUTER_API_KEY exists:", !!OPENROUTER_API_KEY);

const DELAY_MS_MIN = parseInt(process.env.DELAY_MS_MIN ?? "4000", 10);
const DELAY_MS_MAX = parseInt(process.env.DELAY_MS_MAX ?? "7000", 10);
const SAFE_MODE_LIMIT = parseInt(process.env.SAFE_MODE_LIMIT ?? "200", 10);

// Пауза между волнами авто-режима
const AUTO_COOLDOWN_MINUTES = parseInt(process.env.AUTO_COOLDOWN_MINUTES ?? "90", 10);
const AUTO_COOLDOWN_MS = AUTO_COOLDOWN_MINUTES * 60 * 1000;

/* ================== APP ================== */

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// CORS пока максимально простой. При деплое можно сузить список доменов.
app.use(cors({ origin: "*"}));
app.use(bodyParser.json({ limit: "10mb" }));

/* ================== ПУТИ/ФАЙЛЫ ================== */

const DATA_DIR = path.join(process.cwd(), "data");
const OPERATORS_FILE = path.join(DATA_DIR, "operators.json");
const INBOX_FILE = path.join(DATA_DIR, "inbox.json");
const ROUTING_PATH = path.join(process.cwd(), "routing.json");
const BROADCAST_TEMPLATES_FILE = path.join(DATA_DIR, "broadcast_templates.json");
const BROADCAST_CONTACTS_FILE = path.join(DATA_DIR, "broadcast_contacts.json");
const BROADCAST_SCRIPT_FILE = path.join(DATA_DIR, "broadcast_script.json");

// Корень бота (там, где send_pushline.js и results.csv)
function findBotRoot() {
  // стартуем от папки, где лежит index.js / index.ts
  let dir = __dirname;

  for (let i = 0; i < 6; i++) {
    const sendPushline = path.join(dir, "send_pushline.js");
    const results = path.join(dir, "results.csv");

    if (fs.existsSync(sendPushline) || fs.existsSync(results)) {
      console.log("[BOT_ROOT] found:", dir);
      return dir;
    }

    const parent = path.dirname(dir);
    if (parent === dir) break; // дошли до корня диска
    dir = parent;
  }

  console.warn(
    "[BOT_ROOT] send_pushline.js / results.csv not found, fallback to process.cwd()",
    process.cwd()
  );
  return process.cwd();
}

const BOT_ROOT = findBotRoot();
console.log("[BOT_ROOT] using:", BOT_ROOT);

// uploads/ бота (физически туда кладём current_image / current_video)
const UPLOADS_DIR = path.join(BOT_ROOT, "uploads");
const FINAL_IMAGE_PATH = path.join(UPLOADS_DIR, "current_image.jpg");
const FINAL_VIDEO_PATH = path.join(UPLOADS_DIR, "current_video.mp4");

// === Авто-ответы: файл в корне бота ===
const AUTOREPLIES_PATH = path.join(BOT_ROOT, "auto_replies.json");

type AutoReplies = { thanks: string[]; negative: string[] };

function readAutoReplies(): AutoReplies {
  try {
    const j = JSON.parse(fs.readFileSync(AUTOREPLIES_PATH, "utf8"));
    const thanks = Array.isArray(j.thanks) ? j.thanks.filter(Boolean) : [];
    const negative = Array.isArray(j.negative) ? j.negative.filter(Boolean) : [];
    return { thanks, negative };
  } catch {
    return { thanks: [], negative: [] };
  }
}

function writeAutoReplies(next: Partial<AutoReplies>) {
  const cur = readAutoReplies();
  const merged: AutoReplies = {
    thanks: next.thanks !== undefined ? next.thanks : cur.thanks,
    negative: next.negative !== undefined ? next.negative : cur.negative,
  };
  fs.writeFileSync(AUTOREPLIES_PATH, JSON.stringify(merged, null, 2), "utf8");
  return merged;
}

// media_config.json рядом с send_pushline.js
const MEDIA_CFG_PATH = path.join(BOT_ROOT, "media_config.json");

// Раздаём /uploads как статику (чтобы показывать превью)
app.use(
  "/uploads",
  express.static(UPLOADS_DIR, {
    setHeaders: (res) => res.setHeader("Cache-Control", "no-store"),
  })
);

let broadcastMedia = {
  imagePath: fs.existsSync(FINAL_IMAGE_PATH) ? FINAL_IMAGE_PATH : "",
  videoPath: fs.existsSync(FINAL_VIDEO_PATH) ? FINAL_VIDEO_PATH : "",
};

type MediaCfg = {
  imagePath: string;
  videoPath: string;
  imagePaths: string[];
};

// безопасное удаление файла
function safeUnlink(filePath: string) {
  try {
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  } catch (e: any) {
    console.warn("[safeUnlink] fail:", filePath, e?.message || e);
  }
}

function readMediaCfg(): MediaCfg {
  try {
    const raw = fs.readFileSync(MEDIA_CFG_PATH, "utf8");
    const j = JSON.parse(raw);

    let imagePaths: string[] = [];

    if (Array.isArray(j.imagePaths)) {
      imagePaths = j.imagePaths.filter((p: string) => typeof p === "string" && p.length > 0);
    } else if (j.imagePath) {
      imagePaths = [j.imagePath];
    }

    imagePaths = imagePaths.filter((p) => p && fs.existsSync(p));

    const imagePath = j.imagePath || imagePaths[imagePaths.length - 1] || "";
    const videoPath = j.videoPath || "";

    return { imagePath, videoPath, imagePaths };
  } catch {
    return { imagePath: "", videoPath: "", imagePaths: [] };
  }
}

function writeMediaCfgToBotRoot(next: {
  imagePath?: string;
  videoPath?: string;
  imagePaths?: string[];
}) {
  let current: MediaCfg = { imagePath: "", videoPath: "", imagePaths: [] };

  try {
    const raw = fs.readFileSync(MEDIA_CFG_PATH, "utf8");
    const j = JSON.parse(raw);
    current.imagePath = j.imagePath || "";
    current.videoPath = j.videoPath || "";
    current.imagePaths = Array.isArray(j.imagePaths) ? j.imagePaths : [];
  } catch {}

  let imagePaths = current.imagePaths;

  if (next.imagePaths) {
    imagePaths = next.imagePaths;
  } else if (next.imagePath) {
    imagePaths = [...imagePaths, next.imagePath];
  }

  imagePaths = imagePaths.filter((p) => typeof p === "string" && p.length > 0);

  // мягкий лимит, например 50 последних
  const MAX_IMAGES = 50;
  if (imagePaths.length > MAX_IMAGES) {
    imagePaths = imagePaths.slice(-MAX_IMAGES);
  }

  const merged: MediaCfg = {
    imagePath:
      next.imagePath !== undefined
        ? next.imagePath
        : imagePaths[imagePaths.length - 1] || current.imagePath,
    videoPath: next.videoPath !== undefined ? next.videoPath : current.videoPath,
    imagePaths,
  };

  fs.writeFileSync(MEDIA_CFG_PATH, JSON.stringify(merged, null, 2), "utf8");
  return merged;
}

/* ================== Multer для МЕДИА на диск ================== */

const uploadMediaDisk = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const TMP_DIR = path.join(process.cwd(), "uploads_tmp");
      if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR, { recursive: true });
      cb(null, TMP_DIR);
    },
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname) || "";
      const base = file.mimetype.startsWith("video/")
        ? "video-"
        : file.mimetype.startsWith("image/")
        ? "image-"
        : "file-";
      cb(null, base + Date.now() + ext.toLowerCase());
    },
  }),
  limits: { fileSize: 20 * 1024 * 1024 },
});

/* ================== Типы ================== */

type InboxHistoryItem = { who: "client" | "operator"; text: string; at: string };
type InboxMsg = {
  id: string;
  from: string;
  topic: string;
  text: string;
  at: string;
  status: "new" | "routed" | "replied";
  history: InboxHistoryItem[];
  assignedTo?: string;
  unread?: boolean;
};
type Operator = {
  id: string;
  name: string;
  role: "admin" | "operator";
  online: boolean;
  activeChats: number;
  load: number;
  e164?: string;
};

type MediaMode = "image" | "video" | "both" | "text";

type BroadcastRunState = {
  status: "idle" | "running" | "paused" | "done";
  sent: number;
  errors: number;
  startedAt: number | null;
  wavesTotal: number;
  waveIndex: number;
  cooldownUntil: number | null;
  mode: MediaMode;
};

type BroadcastState = {
  contacts: { phone: string; name?: string }[];
  templates: string[];
  run: BroadcastRunState;
};

type Plan = {
  total: number;
  limit: number;
  waves: number;
  lastWaveSize: number;
  avgDelay: number;
  avgWaveMs: number;
  approxTotalMs: number;
};

/* ================== FS helpers ================== */

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}
function readJsonOr<T>(filePath: string, fallback: T): T {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
  } catch {
    return fallback;
  }
}
function writeJson(filePath: string, data: any) {
  ensureDataDir();
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

/* ================== BROADCAST SCRIPT (новый формат) ================== */

export type ScriptStep =
  | {
      type: "text";
      text: string;
      variants?: string[]; // варианты текста для этого шага
    }
  | {
      type: "media";
      mediaType: "image" | "video";
      path: string;
      caption?: string;
      captionVariants?: string[]; // варианты подписи к медиа
    };

function readBroadcastScript(): ScriptStep[] {
  try {
    const raw = fs.readFileSync(BROADCAST_SCRIPT_FILE, "utf8");
    const obj = JSON.parse(raw);
    if (Array.isArray(obj.script)) return obj.script;
    return [];
  } catch {
    return [];
  }
}

function writeBroadcastScript(script: ScriptStep[]) {
  fs.writeFileSync(
    BROADCAST_SCRIPT_FILE,
    JSON.stringify({ script }, null, 2),
    "utf8"
  );
}

/* ================== routing.json ================== */

let routingConfig: {
  enabled: boolean;
  operators: Array<{ name: string; e164: string }>;
  topics: Array<{ topic: string; match: string[] }>;
} = { enabled: false, operators: [], topics: [] };

try {
  routingConfig = JSON.parse(fs.readFileSync(ROUTING_PATH, "utf8"));
} catch {
  console.warn("routing.json not found or invalid, using fallback");
}

/* ================== In-memory ================== */

let inboxMessages: InboxMsg[] = readJsonOr<InboxMsg[]>(INBOX_FILE, [
  {
    id: randomUUID(),
    from: "Андрей К.",
    topic: "Качество роллов",
    text: "Ребята, вчера заказал ещё раз, всё так же топ 🔥 но хотел спросить.",
    at: new Date().toISOString(),
    status: "new",
    history: [
      {
        who: "client",
        text: "Ребята, вчера заказал ещё раз, всё так же топ 🔥 но хотел спросить.",
        at: new Date().toISOString(),
      },
    ],
    unread: true,
  },
]);
function saveInbox() {
  writeJson(INBOX_FILE, inboxMessages);
}

let operators: Operator[] = readJsonOr<Operator[]>(OPERATORS_FILE, []);
if (operators.length === 0) {
  if (routingConfig.operators?.length) {
    operators = routingConfig.operators.map((op, idx) => ({
      id: `op${idx + 1}`,
      name: op.name,
      role: idx === 0 ? "admin" : "operator",
      online: true,
      activeChats: 0,
      load: 0,
      e164: op.e164,
    }));
  } else {
    operators = [
      { id: "op1", name: "Саша", role: "admin", online: true, activeChats: 3, load: 40 },
      { id: "op2", name: "Марина", role: "operator", online: true, activeChats: 5, load: 75 },
      { id: "op3", name: "Игорь", role: "operator", online: false, activeChats: 0, load: 10 },
    ];
  }
  writeJson(OPERATORS_FILE, operators);
}
function saveOperators() {
  writeJson(OPERATORS_FILE, operators);
}

const broadcastState: BroadcastState = {
  contacts: [],
  templates: [],
  run: {
    status: "idle",
    sent: 0,
    errors: 0,
    startedAt: null,
    wavesTotal: 0,
    waveIndex: 0,
    cooldownUntil: null,
    mode: "image",
  },
};

// восстановление состояния
(function loadPersist() {
  try {
    const arrC = JSON.parse(fs.readFileSync(BROADCAST_CONTACTS_FILE, "utf8"));
    if (Array.isArray(arrC)) broadcastState.contacts = arrC;
  } catch {}
  try {
    const arrT = JSON.parse(fs.readFileSync(BROADCAST_TEMPLATES_FILE, "utf8"));
    if (Array.isArray(arrT)) broadcastState.templates = arrT;
  } catch {}
})();
function persistContacts() {
  ensureDataDir();
  fs.writeFileSync(BROADCAST_CONTACTS_FILE, JSON.stringify(broadcastState.contacts, null, 2), "utf8");
}
function persistTemplates() {
  ensureDataDir();
  fs.writeFileSync(BROADCAST_TEMPLATES_FILE, JSON.stringify(broadcastState.templates, null, 2), "utf8");
}

/* ================== helpers ================== */

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function randomBetween(a: number, b: number) {
  return a + Math.floor(Math.random() * (b - a + 1));
}
function detectTopic(text: string): string {
  const msg = (text || "").toLowerCase();
  for (const t of routingConfig.topics || []) {
    if (t.match?.some((kw) => msg.includes(kw.toLowerCase()))) return t.topic;
  }
  return "Общее";
}
function pickRandom<T>(arr: T[]): T | null {
  return arr.length ? arr[Math.floor(Math.random() * arr.length)] : null;
}
function renderTemplateWithName(template: string, name?: string) {
  if (!name) return template.replace(/\{name\}/g, "").replace(/\s+/g, " ").trim();
  return template.replace(/\{name\}/g, name);
}
function computePlan(total: number): Plan {
  const limit = Math.max(1, SAFE_MODE_LIMIT);
  const waves = Math.max(1, Math.ceil(total / limit));
  const lastWaveSize = total % limit === 0 ? limit : total % limit;
  const avgDelay = Math.round((DELAY_MS_MIN + DELAY_MS_MAX) / 2);
  const avgWaveMs = limit * avgDelay;
  const approxTotalMs = (waves - 1) * avgWaveMs + lastWaveSize * avgDelay;
  return { total, limit, waves, lastWaveSize, avgDelay, avgWaveMs, approxTotalMs };
}

function buildMediaByMode(mode: MediaMode) {
  const cfg = readMediaCfg();
  const media: any[] = [];

  if (mode === "text") {
    return media;
  }

  if (mode === "image" || mode === "both") {
    const imgs = (cfg.imagePaths && cfg.imagePaths.length
      ? cfg.imagePaths
      : cfg.imagePath
      ? [cfg.imagePath]
      : []
    ).filter((p) => p && fs.existsSync(p));

    for (const p of imgs) {
      media.push({ type: "image", path: p });
    }
  }

  if ((mode === "video" || mode === "both") && cfg.videoPath && fs.existsSync(cfg.videoPath)) {
    media.push({ type: "video", path: cfg.videoPath });
  }

  return media;
}

/* ================== HEALTH ================== */

app.get("/health", (_req, res) => res.json({ ok: true }));
app.get("/api/health", (_req, res) => res.json({ ok: true }));

/* ================== INBOX API ================== */

app.get("/inbox", (req, res) => res.json(inboxMessages));
app.get("/api/inbox", (req, res) => res.json(inboxMessages));

app.post("/inbox/fake", (req, res) => {
  const id = randomUUID();
  const now = new Date().toISOString();
  const fake: InboxMsg = {
    id,
    from: "Гость " + id.slice(0, 4),
    topic: "Вопрос по доставке",
    text: "Здравствуйте, а можно ли заказать к 21:00?",
    at: now,
    status: "new",
    history: [{ who: "client", text: "Здравствуйте, а можно ли заказать к 21:00?", at: now }],
    unread: true,
  };
  inboxMessages.unshift(fake);
  saveInbox();
  res.json(fake);
});
app.post("/api/inbox/fake", (req, res) => (req.url = "/inbox/fake", (app as any).handle(req, res)));

app.post("/api/ai-assistant", async (req, res) => {
  try {
    if (!OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "OPENROUTER_API_KEY is not set" });
    }

    const { messages } = req.body as {
      messages: { role: "user" | "assistant" | "system"; content: string }[];
    };

    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return res.status(400).json({ error: "messages is required" });
    }

    // System-промпт, чтобы ИИ знал, что такое Pushline
    const systemMessage = {
      role: "system" as const,
      content: `
Ты — AI-помощник внутри панели Pushline Pult.

Контекст проекта:
- Pushline — это система для рассылок в WhatsApp + мини-CRM.
- Есть три части: WhatsApp-бот (send_pushline.js, whatsapp-web.js), backend reply_api, и веб-панель Pushline Pult (React + Vite).
- Бот рассылает сообщения по contacts.csv, использует message_templates.json, auto_replies.json и media_config.json.
- Панель управляет: загрузкой медиа в uploads/, сценариями рассылки (текст/картинка/видео), автоответами, операторами, отчётами.

Твоя задача:
- Помогать с созданием и правкой текстов рассылки (сообщения, цепочки, сценарии).
- Помогать формулировать ключевые слова для NEGATIVE_KEYWORDS и пулов автоответов.
- Объяснять, как устроен код (Node.js, TypeScript, React, whatsapp-web.js) и помогать находить ошибки.
- Давать советы по использованию существующей структуры файлов (contacts.csv, auto_replies.json, routing.json, media_config.json и т.д.).

Формат ответов:
- Отвечай кратко и по делу.
- Если нужен код — давай готовые фрагменты, которые можно вставить.
- Если делаешь рассылочный текст — учитывай, что это WhatsApp, можно использовать эмодзи и дружелюбный тон.
      `.trim(),
    };

    const payload = {
      model: OPENROUTER_MODEL,
      messages: [systemMessage, ...messages],
    };

    const response = await fetch(OPENROUTER_API_BASE, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENROUTER_API_KEY}`,
        "HTTP-Referer": "https://pushline.local", // можешь заменить
        "X-Title": "Pushline AI Assistant",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text();
      console.error("OpenRouter error:", text);
      return res.status(500).json({ error: "OpenRouter request failed", details: text });
    }

    const data = (await response.json()) as any;
    const answer = data.choices?.[0]?.message?.content || "";

    return res.json({ reply: answer });
  } catch (err) {
    console.error("AI assistant error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

const SYNTHETIC_IGNORE_TEXTS = new Set<string>([
  "(медиа без текста)",
  "(media_no_text)",
  "(media without caption)",
]);

function incomingHandler(req: express.Request, res: express.Response) {
  const fromRaw = (req.body?.from || "").trim();
  const textRaw = (req.body?.text || "").trim();
  const atRaw = (req.body?.at || new Date().toISOString()).trim();
  res.json({ ok: true });

  if (!fromRaw) return;
  if (!textRaw) return;
  if (SYNTHETIC_IGNORE_TEXTS.has(textRaw.toLowerCase())) return;

  let chat = inboxMessages.find((m) => m.from === fromRaw);
  if (!chat) {
    chat = {
      id: randomUUID(),
      from: fromRaw,
      topic: detectTopic(textRaw),
      text: textRaw,
      at: atRaw,
      status: "new",
      history: [{ who: "client", text: textRaw, at: atRaw }],
      unread: true,
    };
    inboxMessages.unshift(chat);
  } else {
    chat.topic = detectTopic(textRaw);
    chat.text = textRaw;
    chat.at = atRaw;
    chat.unread = true;
    if (chat.status !== "replied") chat.status = "new";
    chat.history.push({ who: "client", text: textRaw, at: atRaw });
    inboxMessages = [chat, ...inboxMessages.filter((m) => m !== chat)];
  }
  saveInbox();
}
app.post("/wa/incoming", incomingHandler);
app.post("/api/wa/incoming", incomingHandler);

async function inboxReplyHandler(req: express.Request, res: express.Response) {
  const { id } = req.params;
  const { text } = req.body || {};
  if (!text || typeof text !== "string") return res.json({ error: "empty reply" });

  const chat = inboxMessages.find((m) => m.id === id);
  if (!chat) return res.json({ error: "not found" });

  const now = new Date().toISOString();
  chat.history.push({ who: "operator", text, at: now });
  chat.status = "replied";
  chat.text = text;
  chat.at = now;
  chat.unread = false;
  saveInbox();

  try {
    const r = await fetch(`${BOT_API_BASE}/sendReply`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to: chat.from, text }),
    });
    const data = await r.json().catch(() => ({}));
    console.log("[reply -> bot] resp:", data);
  } catch (e: any) {
    console.error("[/inbox/:id/reply] relay fail:", e?.message || e);
  }
  res.json(chat);
}
app.patch("/inbox/:id/reply", inboxReplyHandler);
app.patch("/api/inbox/:id/reply", inboxReplyHandler);

app.patch("/inbox/:id/assign", (req, res) => {
  const { id } = req.params;
  const { operatorId } = req.body || {};
  const chat = inboxMessages.find((m) => m.id === id);
  if (!chat) return res.json({ error: "message not found" });
  const op = operators.find((o) => o.id === operatorId);
  if (!op) return res.json({ error: "operator not found" });

  chat.assignedTo = op.id;
  if (chat.status === "new") chat.status = "routed";
  op.activeChats += 1;
  op.load = Math.min(100, op.load + 5);
  saveInbox();
  saveOperators();
  res.json({ ok: true, message: chat, operator: op });
});
app.patch("/api/inbox/:id/assign", (req, res) => (req.url = `/inbox/${req.params.id}/assign`, (app as any).handle(req, res)));

app.patch("/inbox/:id/read", (req, res) => {
  const { id } = req.params;
  const chat = inboxMessages.find((m) => m.id === id);
  if (chat) {
    chat.unread = false;
    saveInbox();
  }
  res.json({ ok: true });
});
app.patch("/api/inbox/:id/read", (req, res) => (req.url = `/inbox/${req.params.id}/read`, (app as any).handle(req, res)));

app.delete("/inbox/:id", (req, res) => {
  const { id } = req.params;
  const idx = inboxMessages.findIndex((m) => m.id === id);
  if (idx === -1) return res.status(404).json({ ok: false, error: "not found" });
  const [removed] = inboxMessages.splice(idx, 1);
  saveInbox();
  return res.json({ ok: true, removedId: removed.id });
});
app.delete("/api/inbox/:id", (req, res) => (req.url = `/inbox/${req.params.id}`, (app as any).handle(req, res)));

app.delete("/inbox", (req, res) => {
  const count = inboxMessages.length;
  inboxMessages = [];
  saveInbox();
  return res.json({ ok: true, removedCount: count });
});
app.delete("/api/inbox", (req, res) => (req.url = "/inbox", (app as any).handle(req, res)));

/* ================== OPERATORS ================== */

app.get("/operators", (req, res) => res.json(operators));
app.get("/api/operators", (req, res) => res.json(operators));

app.patch("/operators/:id", (req, res) => {
  const { id } = req.params;
  const patch = req.body || {};
  const op = operators.find((o) => o.id === id);
  if (!op) return res.json({ error: "not found" });

  if (typeof patch.online === "boolean") op.online = patch.online;
  if (typeof patch.activeChats === "number") op.activeChats = Math.max(0, patch.activeChats);
  if (typeof patch.load === "number") op.load = Math.min(100, Math.max(0, patch.load));
  if (typeof patch.name === "string" && patch.name.trim()) op.name = patch.name.trim();
  if (typeof patch.role === "string" && (patch.role === "admin" || patch.role === "operator")) {
    op.role = patch.role;
  }
  saveOperators();
  res.json(op);
});
app.patch("/api/operators/:id", (req, res) => (req.url = `/operators/${req.params.id}`, (app as any).handle(req, res)));

/* ================== MEDIA UPLOAD ================== */

function uploadImageHandler(req: express.Request, res: express.Response) {
  if (!(req as any).file) return res.json({ ok: false, error: "no_file" });

  try {
    if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });

    const file = (req as any).file as Express.Multer.File;
    const ext = path.extname(file.originalname || "") || ".jpg";

    const destPath = path.join(
      UPLOADS_DIR,
      `img-${Date.now()}-${Math.random().toString(36).slice(2, 8)}${ext.toLowerCase()}`
    );

    fs.copyFileSync(file.path, destPath);

    broadcastMedia.imagePath = destPath;

    const curCfg = readMediaCfg();

    // копим все картинки, а ограничение по количеству
    // уже делает writeMediaCfgToBotRoot (MAX_IMAGES)
    let nextImagePaths = [...curCfg.imagePaths, destPath].filter(
      (p) => p && fs.existsSync(p)
    );

    writeMediaCfgToBotRoot({
      imagePath: destPath,
      imagePaths: nextImagePaths,
    });

    return res.json({
      ok: true,
      type: "image",
      filename: path.basename(destPath),
      size: file.size,
    });
  } catch (err: any) {
    console.error("upload-media/image copy fail:", err?.message || err);
    return res.json({ ok: false, error: "copy_fail" });
  }
}
function uploadVideoHandler(req: express.Request, res: express.Response) {
  if (!(req as any).file) return res.json({ ok: false, error: "no_file" });
  try {
    if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });
    fs.copyFileSync((req as any).file.path, FINAL_VIDEO_PATH);
    broadcastMedia.videoPath = FINAL_VIDEO_PATH;
    writeMediaCfgToBotRoot({ videoPath: FINAL_VIDEO_PATH });
    return res.json({
      ok: true,
      type: "video",
      filename: path.basename(FINAL_VIDEO_PATH),
      size: (req as any).file.size,
    });
  } catch (err: any) {
    console.error("upload-media/video copy fail:", err?.message || err);
    return res.json({ ok: false, error: "copy_fail" });
  }
}

app.post("/upload-media/image", uploadMediaDisk.single("file"), uploadImageHandler);
app.post("/upload-media/video", uploadMediaDisk.single("file"), uploadVideoHandler);
app.post("/api/upload-media/image", uploadMediaDisk.single("file"), uploadImageHandler);
app.post("/api/upload-media/video", uploadMediaDisk.single("file"), uploadVideoHandler);

app.get("/broadcast/media", (req, res) => {
  const cfg = readMediaCfg();

  const toWeb = (fullPath: string | "") =>
    fullPath ? `/uploads/${path.basename(fullPath)}` : "";

  // все картинки, которые есть в media_config.json
  const imagesArr = (cfg.imagePaths || [])
    .filter((p) => p && fs.existsSync(p))
    .map((p) => ({
      filename: path.basename(p),
      webUrl: toWeb(p), // для превью в браузере
      path: p,          // ПОЛНЫЙ путь на диске — этим будет пользоваться сценарий
    }));

  const mainImage =
    imagesArr.length > 0
      ? imagesArr[imagesArr.length - 1]
      : cfg.imagePath && fs.existsSync(cfg.imagePath)
      ? {
          filename: path.basename(cfg.imagePath),
          webUrl: toWeb(cfg.imagePath),
          path: cfg.imagePath,
        }
      : null;

  const vid =
    cfg.videoPath && fs.existsSync(cfg.videoPath)
      ? {
          filename: path.basename(cfg.videoPath),
          webUrl: toWeb(cfg.videoPath),
          path: cfg.videoPath, // тоже полный путь
        }
      : null;

  res.json({
    ok: true,
    image: mainImage,
    images: imagesArr,
    video: vid,
  });
});
app.get("/api/broadcast/media", (req, res) => (req.url = "/broadcast/media", (app as any).handle(req, res)));

/* ====== CLEAR MEDIA (очистка медиа для рассылки) ====== */

function clearBroadcastMediaHandler(_req: express.Request, res: express.Response) {
  try {
    console.log("[broadcast/media/clear] requested");

    // 0) читаем текущий конфиг, чтобы удалить именно те файлы, которые в нём записаны
    const cfg = readMediaCfg();

    // 1) удаляем файлы из media_config.json (если они есть)
    if (cfg.imagePath) {
      safeUnlink(cfg.imagePath);
    }
    if (cfg.videoPath) {
      safeUnlink(cfg.videoPath);
    }
    if (Array.isArray(cfg.imagePaths)) {
      for (const p of cfg.imagePaths) {
        if (p) safeUnlink(p);
      }
    }

    // 2) дополнительно чистим uploads/ по маскам (current_*, img-*, video-*)
    if (fs.existsSync(UPLOADS_DIR)) {
      const files = fs.readdirSync(UPLOADS_DIR);
      for (const f of files) {
        const full = path.join(UPLOADS_DIR, f);
        if (
          f === "current_image.jpg" ||
          f === "current_video.mp4" ||
          f.startsWith("img-") ||
          f.startsWith("video-")
        ) {
          safeUnlink(full);
        }
      }
    }

    // 3) сбрасываем media_config.json (то, что читает send_pushline.js)
    writeMediaCfgToBotRoot({
      imagePath: "",
      videoPath: "",
      imagePaths: [],
    });

    // 4) сбрасываем in-memory состояние
    broadcastMedia.imagePath = "";
    broadcastMedia.videoPath = "";

    console.log("[broadcast/media/clear] done -> ok");
    return res.json({ ok: true });
  } catch (err: any) {
    console.error("[broadcast/media/clear] fail:", err?.message || err);
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err),
    });
  }
}

// Оба маршрута явно навешиваем на один и тот же handler
app.post("/broadcast/media/clear", clearBroadcastMediaHandler);
app.post("/api/broadcast/media/clear", clearBroadcastMediaHandler);

/* ================== UPLOAD CONTACTS / TEMPLATES ================== */

function normalizePhone(val: string) {
  let p = (val || "").replace(/[^\d+]/g, "");
  if (p.startsWith("8")) p = "+7" + p.slice(1);
  else if (p.startsWith("7")) p = "+7" + p.slice(1);
  if (!p.startsWith("+") && p.length >= 10) p = "+" + p;
  return p;
}
function looksLikePhone(val: string) {
  const p = normalizePhone(val);
  return /^\+?\d{10,15}$/.test(p);
}
function splitRow(line: string) {
  if (line.includes("\t")) return line.split("\t").map((s) => s.trim());
  if (line.includes(";")) return line.split(";").map((s) => s.trim());
  return line.split(",").map((s) => s.trim());
}

function uploadContactsHandler(req: express.Request, res: express.Response) {
  if (!(req as any).file) return res.json({ ok: false, error: "no file" });

  broadcastState.run = {
    status: "idle",
    sent: 0,
    errors: 0,
    startedAt: null,
    wavesTotal: 0,
    waveIndex: 0,
    cooldownUntil: null,
    mode: "image",
  };
  broadcastState.contacts = [];

  const raw = (req as any).file.buffer.toString("utf8");
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter((l) => l.length > 0);

  const startIdx = lines[0] && /phone/i.test(lines[0]) ? 1 : 0;

  let added = 0;
  for (let i = startIdx; i < lines.length; i++) {
    const parts = splitRow(lines[i]);
    if (!parts.length) continue;

    const colA = parts[0] || "";
    const colB = parts[1] || "";

    let phone = "";
    let name = "";

    if (looksLikePhone(colA) && !looksLikePhone(colB)) {
      phone = normalizePhone(colA);
      name = colB || "";
    } else if (!looksLikePhone(colA) && looksLikePhone(colB)) {
      phone = normalizePhone(colB);
      name = colA || "";
    } else if (looksLikePhone(colA) && looksLikePhone(colB)) {
      phone = normalizePhone(colA);
      name = colB || "";
    } else continue;

    if (!phone) continue;

    broadcastState.contacts.push({ phone, name: name || undefined });
    added++;
  }
  persistContacts();

  const plan = computePlan(broadcastState.contacts.length);
  return res.json({ ok: true, rows: added, totalContacts: broadcastState.contacts.length, plan });
}
app.post("/upload-contacts", upload.single("file"), uploadContactsHandler);
app.post("/api/contacts/upload", upload.single("file"), uploadContactsHandler);

function uploadTemplatesHandler(req: express.Request, res: express.Response) {
  if (!(req as any).file) return res.json({ ok: false, error: "no file" });

  broadcastState.run = {
    status: "idle",
    sent: 0,
    errors: 0,
    startedAt: null,
    wavesTotal: 0,
    waveIndex: 0,
    cooldownUntil: null,
    mode: "image",
  };

  const buf = (req as any).file.buffer.toString("utf8").trim();
  const filename = (req as any).file.originalname.toLowerCase();

  let newTemplates: string[] = [];
  try {
    if (filename.endsWith(".json")) {
      const parsed = JSON.parse(buf);
      if (Array.isArray(parsed)) {
        newTemplates = parsed.filter((x) => typeof x === "string").map((x) => x.trim()).filter(Boolean);
      } else if (parsed && Array.isArray((parsed as any).templates)) {
        newTemplates = (parsed as any).templates
          .filter((x: any) => typeof x === "string")
          .map((x: string) => x.trim())
          .filter(Boolean);
      }
    } else {
      newTemplates = buf.split(/\n\s*\n|---+|===+/g).map((c) => c.trim()).filter(Boolean);
    }
  } catch {
    return res.json({ ok: false, error: "parse error" });
  }

  broadcastState.templates = newTemplates.slice();
  persistTemplates();

  const plan = computePlan(broadcastState.contacts.length);
  return res.json({ ok: true, templates: newTemplates.length, totalTemplates: broadcastState.templates.length, plan });
}
app.post("/upload-templates", upload.single("file"), uploadTemplatesHandler);
app.post("/api/templates/upload", upload.single("file"), uploadTemplatesHandler);

app.get("/templates", (req, res) => {
  res.json({ ok: true, templates: broadcastState.templates });
});
app.get("/api/templates", (req, res) => (req.url = "/templates", (app as any).handle(req, res)));

/* ================== BROADCAST SCRIPT API (новый) ================== */

app.get("/broadcast/script", (req, res) => {
  const script = readBroadcastScript();
  res.json({ ok: true, script });
});

app.post("/broadcast/script", (req, res) => {
  const { script } = req.body || {};
  if (!Array.isArray(script)) {
    return res.status(400).json({ ok: false, error: "script must be array" });
  }

  const normalized = script
    .map((s: any) => {
      if (!s || typeof s !== "object") return null;

      if (s.type === "text") {
        if (typeof s.text !== "string") return null;

        const variants = Array.isArray(s.variants)
          ? s.variants
              .filter((x: any) => typeof x === "string")
              .map((x: string) => x.trim())
              .filter(Boolean)
          : [];

        const obj: any = { type: "text", text: s.text };
        if (variants.length) obj.variants = variants;
        return obj;
      }

      if (s.type === "media") {
        if (s.mediaType !== "image" && s.mediaType !== "video") return null;
        if (typeof s.path !== "string") return null;

        const caption =
          typeof s.caption === "string" ? s.caption.trim() : "";

        const captionVariants = Array.isArray(s.captionVariants)
          ? s.captionVariants
              .filter((x: any) => typeof x === "string")
              .map((x: string) => x.trim())
              .filter(Boolean)
          : [];

        const obj: any = {
          type: "media",
          mediaType: s.mediaType,
          path: s.path,
        };

        if (caption) obj.caption = caption;
        if (captionVariants.length) obj.captionVariants = captionVariants;

        return obj;
      }

      return null;
    })
    .filter(Boolean) as ScriptStep[];

  writeBroadcastScript(normalized);
  res.json({ ok: true, saved: normalized.length });
});

// ================== AUTO-REPLIES ==================

function uploadAutoRepliesHandler(req: express.Request, res: express.Response) {
  if (!(req as any).file) return res.json({ ok: false, error: "no file" });

  const buf = (req as any).file.buffer.toString("utf8");
  const filename = ((req as any).file.originalname || "").toLowerCase();

  let thanks: string[] = [];
  let negative: string[] = [];

  try {
    if (filename.endsWith(".json")) {
      const j = JSON.parse(buf);
      if (Array.isArray(j)) {
        thanks = j.filter((x: any) => typeof x === "string").map((s: string) => s.trim()).filter(Boolean);
      } else {
        if (Array.isArray(j.thanks))
          thanks = j.thanks.filter((x: any) => typeof x === "string").map((s: string) => s.trim()).filter(Boolean);
        if (Array.isArray(j.negative))
          negative = j.negative.filter((x: any) => typeof x === "string").map((s: string) => s.trim()).filter(Boolean);
      }
    } else {
      const split = buf.split(/^\s*===\s*NEGATIVE\s*===\s*$/im);
      const left = split[0] || "";
      const right = split[1] || "";

      thanks = left.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
      if (right) {
        negative = right.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
      }
    }
  } catch {
    return res.json({ ok: false, error: "parse error" });
  }

  const merged = writeAutoReplies({ thanks, negative });
  return res.json({
    ok: true,
    counts: { thanks: merged.thanks.length, negative: merged.negative.length },
  });
}
app.post("/upload-autoreplies", upload.single("file"), uploadAutoRepliesHandler);
app.post("/api/autoreplies/upload", upload.single("file"), uploadAutoRepliesHandler);

app.get("/autoreplies", (req, res) => {
  const cur = readAutoReplies();
  res.json({
    ok: true,
    counts: { thanks: cur.thanks.length, negative: cur.negative.length },
    preview: {
      thanks: cur.thanks.slice(0, 3),
      negative: cur.negative.slice(0, 3),
    },
  });
});
app.get("/api/autoreplies", (req, res) => (req.url = "/autoreplies", (app as any).handle(req, res)));

/* ================== BROADCAST ================== */

function getBroadcastStatus() {
  const { status, sent, errors, startedAt, wavesTotal, waveIndex, cooldownUntil, mode } = broadcastState.run;
  const total = broadcastState.contacts.length;
  return {
    ok: true,
    status,
    sent,
    errors,
    total,
    startedAt,
    wavesTotal,
    waveIndex,
    cooldownUntil,
    mode,
    plan: computePlan(total),
  };
}

app.get("/broadcast/plan", (req, res) => {
  const total = broadcastState.contacts.length;
  return res.json({ ok: true, plan: computePlan(total) });
});
app.get("/api/broadcast/plan", (req, res) => (req.url = "/broadcast/plan", (app as any).handle(req, res)));

app.get("/broadcast/status", (req, res) => res.json(getBroadcastStatus()));
app.get("/api/broadcast/status", (req, res) => (req.url = "/broadcast/status", (app as any).handle(req, res)));

async function sendOne(
  to: string,
  name: string | undefined,
  fallbackText: string,
  mode: MediaMode
): Promise<boolean> {
  // 1) Пытаемся найти сценарий (blocks -> script)
  const rawScript = readBroadcastScript();
  if (rawScript.length > 0) {
    try {
      const script: ScriptStep[] = rawScript.map((s: any) => {
        if (s.type === "text") {
          let base = typeof s.text === "string" ? s.text : "";

          if (Array.isArray(s.variants) && s.variants.length > 0) {
            const cleanVariants = s.variants
              .filter((x: any) => typeof x === "string")
              .map((x: string) => x.trim())
              .filter(Boolean);

            const chosen = pickRandom(cleanVariants);
            if (chosen) base = chosen;
          }

          const finalText = renderTemplateWithName(base, name);
          return { type: "text", text: finalText };
        }

        if (s.type === "media") {
          const out: any = {
            type: "media",
            mediaType: s.mediaType,
            path: s.path,
          };

          let baseCaption = "";
          if (typeof s.caption === "string") {
            baseCaption = s.caption;
          }

          if (Array.isArray(s.captionVariants) && s.captionVariants.length > 0) {
            const clean = s.captionVariants
              .filter((x: any) => typeof x === "string")
              .map((x: string) => x.trim())
              .filter(Boolean);

            const chosen = pickRandom(clean);
            if (chosen) baseCaption = chosen;
          }

          if (baseCaption) {
            out.caption = renderTemplateWithName(baseCaption, name);
          }

          return out;
        }

        return s as ScriptStep;
      });

      const payload = { to, script };
      const r = await fetch(`${BOT_API_BASE}/sendDirect`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const j = await r.json().catch(() => ({}));
      const ok = !!j?.ok;

      appendHistoryRow(
        to,
        name,
        ok ? "SENT_OK" : "ERROR_SEND",
        ok ? "SCRIPT" : `SCRIPT_FAIL:${j?.error || r.status}`
      );
      if (ok) markSentCache(to);

      return ok;
    } catch (e: any) {
      appendHistoryRow(
        to,
        name,
        "ERROR_SEND",
        `SCRIPT_EXCEPTION:${e?.message || String(e)}`
      );
      return false;
    }
  }

  // 2) Если сценария нет — старая логика: text + media
  try {
    const mediaPayload = buildMediaByMode(mode);
    const payloadForBot: any = { to, text: fallbackText };
    if (mediaPayload.length) payloadForBot.media = mediaPayload;

    const r = await fetch(`${BOT_API_BASE}/sendDirect`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payloadForBot),
    });
    const j = await r.json().catch(() => ({}));
    const ok = (r as any).ok && j?.ok;

    appendHistoryRow(
      to,
      name,
      ok ? "SENT_OK" : "ERROR_SEND",
      ok ? "LEGACY" : `LEGACY_FAIL:${j?.error || r.status}`
    );
    if (ok) markSentCache(to);

    return !!ok;
  } catch (e: any) {
    appendHistoryRow(
      to,
      name,
      "ERROR_SEND",
      `LEGACY_EXCEPTION:${e?.message || String(e)}`
    );
    return false;
  }
}

function isRunning() {
  return broadcastState.run.status === "running";
}

async function waveHandler(req: express.Request, res: express.Response) {
  const { adminPin, mode } = (req.body || {}) as any;
  if (!adminPin || adminPin !== ADMIN_PIN) {
    return res.status(403).json({ ok: false, error: "forbidden" });
  }

  const hasScript = readBroadcastScript().length > 0;
  if (!broadcastState.templates.length && !hasScript) {
    return res.json({ ok: false, error: "no templates or script loaded" });
  }
  if (!broadcastState.contacts.length) {
    return res.json({ ok: false, error: "no contacts loaded" });
  }

  const modeFromReq: MediaMode =
    mode === "video" || mode === "both" || mode === "text" || mode === "image" ? mode : "image";

  if (broadcastState.run.status !== "running") {
    const plan = computePlan(broadcastState.contacts.length);
    broadcastState.run = {
      status: "running",
      sent: 0,
      errors: 0,
      startedAt: Date.now(),
      wavesTotal: plan.waves,
      waveIndex: Math.max(1, broadcastState.run.waveIndex || 1),
      cooldownUntil: null,
      mode: modeFromReq,
    };
  } else {
    broadcastState.run.mode = modeFromReq;
  }

  const limit = Math.max(1, SAFE_MODE_LIMIT);
  const toProcess = broadcastState.contacts.slice(0, limit);

  const scriptExists = readBroadcastScript().length > 0;

  for (const c of toProcess) {
    if (!isRunning()) break;

    let text = "";

    // Если НЕТ script – работаем по старому (через шаблоны)
    if (!scriptExists) {
      const tpl = pickRandom(broadcastState.templates);
      if (!tpl) continue;
      text = renderTemplateWithName(tpl, c.name);
    }

    try {
      const ok = await sendOne(
        c.phone,
        c.name,
        text,
        broadcastState.run.mode || "image"
      );
      if (ok) broadcastState.run.sent++;
      else broadcastState.run.errors++;
    } catch {
      broadcastState.run.errors++;
    }

    const delay = randomBetween(DELAY_MS_MIN, DELAY_MS_MAX);
    for (let passed = 0; passed < delay; passed += 200) {
      if (!isRunning()) break;
      await sleep(200);
    }
    if (!isRunning()) break;
  }

  const processedCount = toProcess.findIndex((_, i) => !isRunning() && i >= 0) >= 0 ? 0 : toProcess.length;
  broadcastState.contacts = broadcastState.contacts.slice(processedCount);
  persistContacts();

  if (broadcastState.contacts.length === 0) {
    broadcastState.run.status = "done";
  } else {
    if (isRunning()) {
      broadcastState.run.waveIndex = Math.min(broadcastState.run.waveIndex + 1, broadcastState.run.wavesTotal);
    }
  }

  return res.json(getBroadcastStatus());
}
app.post("/broadcast/wave", waveHandler);
app.post("/api/broadcast/wave", waveHandler);

async function fireHandler(req: express.Request, res: express.Response) {
  const { adminPin, mode } = (req.body || {}) as any;
  if (!adminPin || adminPin !== ADMIN_PIN) {
    return res.status(403).json({ ok: false, error: "forbidden" });
  }

  const hasScript = readBroadcastScript().length > 0;
  if (!broadcastState.templates.length && !hasScript) {
    return res.json({ ok: false, error: "no templates or script loaded" });
  }
  if (!broadcastState.contacts.length) {
    return res.json({ ok: false, error: "no contacts loaded" });
  }

  const modeFromReq: MediaMode =
    mode === "video" || mode === "both" || mode === "text" || mode === "image"
      ? mode
      : "image";

  const plan = computePlan(broadcastState.contacts.length);
  res.json({ ok: true, started: true, plan });

  // инициализируем состояние забега
  broadcastState.run.status = "running";
  broadcastState.run.startedAt = Date.now();
  broadcastState.run.sent = 0;
  broadcastState.run.errors = 0;
  broadcastState.run.wavesTotal = plan.waves;
  broadcastState.run.waveIndex = 1;
  broadcastState.run.cooldownUntil = null;
  broadcastState.run.mode = modeFromReq;

  const limit = Math.max(1, SAFE_MODE_LIMIT);
  const scriptExists = readBroadcastScript().length > 0;

  // основной цикл по "волнам"
  while (broadcastState.contacts.length > 0 && isRunning()) {
    const batch = broadcastState.contacts.slice(0, limit);

    // проходимся по каждому контакту в батче
    for (const c of batch) {
      if (!isRunning()) break;

      let text = "";

      // если скрипта нет — берём текст из шаблонов как раньше
      if (!scriptExists) {
        const tpl = pickRandom(broadcastState.templates);
        if (!tpl) continue;
        text = renderTemplateWithName(tpl, c.name);
      }

      try {
        const ok = await sendOne(
          c.phone,
          c.name,
          text,
          broadcastState.run.mode || "image"
        );
        if (ok) broadcastState.run.sent++;
        else broadcastState.run.errors++;
      } catch {
        broadcastState.run.errors++;
      }

      const delay = randomBetween(DELAY_MS_MIN, DELAY_MS_MAX);
      for (let passed = 0; passed < delay; passed += 200) {
        if (!isRunning()) break;
        await sleep(200);
      }
      if (!isRunning()) break;
    }

    // урезаем очередь контактов на размер батча
    const actuallySentOrTried = batch.length;
    broadcastState.contacts = broadcastState.contacts.slice(actuallySentOrTried);
    persistContacts();

    // если ещё остались контакты и мы всё ещё "running" — включаем cooldown
    if (broadcastState.contacts.length > 0 && isRunning()) {
      broadcastState.run.waveIndex = Math.min(
        broadcastState.run.waveIndex + 1,
        broadcastState.run.wavesTotal
      );

      const until = Date.now() + AUTO_COOLDOWN_MS;
      broadcastState.run.cooldownUntil = until;

      while (Date.now() < until && isRunning()) {
        await sleep(1000);
      }
      broadcastState.run.cooldownUntil = null;
    }
  }

  if (isRunning()) {
    broadcastState.run.status = "done";
  }
}

app.post("/broadcast/fire", fireHandler);
app.post("/api/broadcast/fire", fireHandler);

/* ====== История рассылок ====== */

const RESULTS_CSV = path.join(BOT_ROOT, "results.csv");
const SENT_CACHE_FILE = path.join(BOT_ROOT, "sent_cache.json");

type HistoryRow = {
  timestamp: string;
  phone: string;
  name: string;
  status: string;
  details: string;
};

function ensureResultsHeader() {
  if (!fs.existsSync(RESULTS_CSV)) {
    const header = "timestamp,phone,name,status,details\n";
    fs.writeFileSync(RESULTS_CSV, header, "utf8");
  }
}

function appendHistoryRow(
  phone: string,
  name: string | undefined,
  status: string,
  details: string
) {
  ensureResultsHeader();
  const ts = new Date().toISOString().replace("T", " ").replace("Z", "");
  const safeName = (name || "").replace(/,/g, " ");
  const safeDetails = (details || "").replace(/,/g, " ");
  const line = `${ts},${phone},${safeName},${status},${safeDetails}\n`;
  fs.appendFileSync(RESULTS_CSV, line, "utf8");
}

function markSentCache(phone: string) {
  let cache: Record<string, boolean> = {};
  try {
    const raw = fs.readFileSync(SENT_CACHE_FILE, "utf8");
    cache = JSON.parse(raw);
  } catch {
    cache = {};
  }
  cache[phone] = true;
  fs.writeFileSync(SENT_CACHE_FILE, JSON.stringify(cache, null, 2), "utf8");
}

function readResultsCsv(limit = 500): HistoryRow[] {
  if (!fs.existsSync(RESULTS_CSV)) return [];
  const raw = fs.readFileSync(RESULTS_CSV, "utf8").trim().split(/\r?\n/);
  if (raw.length <= 1) return [];
  const rows = raw.slice(1);
  const pick = rows.slice(-limit);
  return pick
    .map((line) => {
      const [timestamp, phone, name, status, details] = line.split(",");
      if (!timestamp || !phone) return null;
      return { timestamp, phone, name, status, details };
    })
    .filter(Boolean) as HistoryRow[];
}

// ищем «последнюю волну» — подряд идущие SENT_OK с конца файла
function detectLastWave(records: HistoryRow[]): HistoryRow[] {
  // создаём копию массива и идём с конца
  const rev = [...records].reverse();
  const wave: HistoryRow[] = [];

  for (const r of rev) {
    if (r.status !== "SENT_OK") break;
    wave.push(r);
  }

  return wave.reverse(); // обратно в хронологический порядок
}

app.get("/api/broadcast/last", (req, res) => {
  const limit = Math.min(
    5000,
    parseInt(String(req.query.limit ?? "500"), 10) || 500
  );
  const data = readResultsCsv(limit);
  res.json({ ok: true, count: data.length, data });
});

app.get("/api/broadcast/last-wave", (req, res) => {
  const tail = readResultsCsv(5000);
  const wave = detectLastWave(tail);
  res.json({
    ok: true,
    total: wave.length,
    phones: Array.from(new Set(wave.map((r) => r.phone))),
    records: wave,
  });
});

app.get("/api/broadcast/sent-cache", (req, res) => {
  try {
    const j = JSON.parse(fs.readFileSync(SENT_CACHE_FILE, "utf8"));
    const phones = Object.keys(j).filter((k) => j[k] === true);
    res.json({ ok: true, total: phones.length, phones });
  } catch {
    res.json({ ok: false, error: "NO_CACHE" });
  }
});

/* ================== ТЕСТОВЫЕ отправки ================== */

async function testDirectHandler(req: express.Request, res: express.Response) {
  try {
    const { to, text, mode } = (req.body || {}) as any;
    if (!to) return res.status(400).json({ ok: false, error: "to required" });

    // 1) Если есть сценарий — тестируем именно его (как есть)
    const script = readBroadcastScript();
    if (script.length > 0) {
      const r = await fetch(`${BOT_API_BASE}/sendDirect`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ to, script }),
      });
      const data = await r.json().catch(() => ({}));
      return res.json(data);
    }

    // 2) Если сценария нет — старая логика (текст + медиа)
    const modeFromReq: MediaMode =
      mode === "video" || mode === "both" || mode === "text" || mode === "image" ? mode : "image";

    const media = buildMediaByMode(modeFromReq);

    const r = await fetch(`${BOT_API_BASE}/sendDirect`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to, text: text || "", media }),
    });
    const data = await r.json().catch(() => ({}));
    return res.json(data);
  } catch (err: any) {
    console.error("[test-direct] fail:", err?.message || err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}
app.post("/broadcast/test-direct", testDirectHandler);
app.post("/api/broadcast/test-direct", testDirectHandler);

/* ================== STATUS helpers ================== */

function stopHandler(_req: express.Request, res: express.Response) {
  broadcastState.run.status = "done";
  broadcastState.run.cooldownUntil = null;
  return res.json(getBroadcastStatus());
}
app.post("/broadcast/stop", stopHandler);
app.post("/api/broadcast/stop", stopHandler);

function pauseHandler(_req: express.Request, res: express.Response) {
  if (broadcastState.run.status === "running") broadcastState.run.status = "paused";
  return res.json(getBroadcastStatus());
}
app.post("/broadcast/pause", pauseHandler);
app.post("/api/broadcast/pause", pauseHandler);

function resetHandler(_req: express.Request, res: express.Response) {
  broadcastState.run = {
    status: "idle",
    sent: 0,
    errors: 0,
    startedAt: null,
    wavesTotal: 0,
    waveIndex: 0,
    cooldownUntil: null,
    mode: "image",
  };
  return res.json(getBroadcastStatus());
}
app.post("/broadcast/reset", resetHandler);
app.post("/api/broadcast/reset", resetHandler);

/* ================== START ================== */

app.listen(PORT, () => {
  console.log(`Pushline backend running on http://localhost:${PORT}`);
});
