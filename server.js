require("dotenv").config();
const fs = require("fs");
const express = require("express");
const axios = require("axios");
const { Worker } = require("worker_threads");
const csv = require("csv-parser");
const { createClient } = require("@supabase/supabase-js");

const app = express();

// ================= CONFIG =================
const BASE_URL = "https://bzl.api.sellercloud.com/rest/api/Inventory";
const PAGE_SIZE = 50;
const WORKERS = 5;

const OUTPUT = "Inventory.csv";
const STATE_FILE = "state.json";
const WRITTEN_FILE = "written.json";

// ================= SUPABASE =================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ================= GLOBAL STATE =================
let CURRENT_TOKEN = process.env.SELLERCLOUD_TOKEN;

let allowWorkers = false;
let jobFinished = false;
let refreshing = false;

let currentPage = 1;
let emptyPageCount = 0;
let totalSkipped = 0;

let written = new Set();
let workerStats = {};
let warehouseStats = {};

let stream = null;
let headerWritten = false;

// ================= TOKEN =================
async function refreshToken() {
  if (refreshing) return;
  refreshing = true;

  try {
    const res = await axios.post(
      "https://bzl.api.sellercloud.com/rest/api/token",
      {
        username: process.env.SC_USERNAME,
        password: process.env.SC_PASSWORD
      }
    );
    CURRENT_TOKEN = res.data.access_token;
    console.log("✅ Token refreshed");
  } catch {
    console.log("❌ Token refresh failed");
  } finally {
    refreshing = false;
  }
}

// ================= SUPABASE BUCKET AUTO CREATE =================
async function ensureBucketExists(bucketName) {
  try {
    const { data: buckets, error } =
      await supabase.storage.listBuckets();

    if (error) throw error;

    const exists = buckets.some(b => b.name === bucketName);

    if (!exists) {
      console.log(`📦 Creating bucket: ${bucketName}`);
      const { error: createError } =
        await supabase.storage.createBucket(bucketName, {
          public: false
        });

      if (createError) throw createError;
      console.log(`✅ Bucket "${bucketName}" created`);
    } else {
      console.log(`📁 Bucket "${bucketName}" exists`);
    }
  } catch (err) {
    console.error("❌ Bucket error:", err.message);
    throw err;
  }
}

// ================= CSV =================
function writeCSV(rows) {
  if (!rows.length) return;

  if (!headerWritten) {
    stream.write(Object.keys(rows[0]).join(",") + "\n");
    headerWritten = true;
  }

  for (const row of rows) {
    if (!row.ID || written.has(row.ID)) continue;
    written.add(row.ID);

    stream.write(
      Object.values(row)
        .map(v => `"${String(v ?? "").replace(/"/g, '""')}"`)
        .join(",") + "\n"
    );
  }
}

// ================= UI =================
function render() {
  console.clear();
  console.log("🚀 SellerCloud Inventory Export\n");

  console.log(`📄 Page: ${currentPage}`);
  console.log(`📦 Saved: ${written.size}`);
  console.log(`⏭ Skipped: ${totalSkipped}`);
  console.log(`🧱 Empty Pages: ${emptyPageCount}\n`);

  console.log("🏬 Warehouse Totals:");
  for (const [w, c] of Object.entries(warehouseStats)) {
    console.log(`   • ${w}: ${c}`);
  }

  console.log("\n🧵 Workers:");
  for (const [id, w] of Object.entries(workerStats)) {
    console.log(
      `   Worker ${id} → Page ${w.page} | Saved: ${w.active} | Skipped: ${w.skipped}`
    );
  }
}

// ================= WORKER =================
function spawnWorker(id, page) {
  if (!allowWorkers) return;

  const worker = new Worker("./worker.js", {
    workerData: {
      BASE_URL,
      TOKEN: CURRENT_TOKEN,
      PAGE_SIZE,
      page
    }
  });

  worker.on("message", async msg => {
    if (!allowWorkers) return;

    if (msg.type === "expired") {
      await refreshToken();
      spawnWorker(id, page);
      return;
    }

    workerStats[id] = {
      page: msg.page,
      active: msg.activeItems.length,
      skipped: msg.skipped
    };

    writeCSV(msg.activeItems);
    totalSkipped += msg.skipped;

    for (const [w, c] of Object.entries(msg.warehouseCount || {})) {
      warehouseStats[w] = (warehouseStats[w] || 0) + c;
    }

    const isEmpty =
      msg.activeItems.length === 0 &&
      msg.skipped === 0;

    emptyPageCount = isEmpty ? emptyPageCount + 1 : 0;

    render();

    // ================= STOP CONDITION =================
    if (emptyPageCount >= 5 && !jobFinished) {
      jobFinished = true;
      allowWorkers = false;

      console.log("\n✅ All pages processed");
      console.log("⏳ Finalizing...");

      setTimeout(async () => {
        stream.end(async () => {
          const finalName = `Inventory-${new Date()
            .toISOString()
            .replace(/:/g, "-")}.csv`;

          if (fs.existsSync(OUTPUT)) {
            fs.renameSync(OUTPUT, finalName);
            console.log(`📁 Saved as ${finalName}`);
          }

          // ================= SUPABASE FLOW =================
          try {
            console.log("☁ Uploading to Supabase...");

            await ensureBucketExists("inventory");

            const fileBuffer = fs.readFileSync(finalName);

            await supabase.storage
              .from("inventory")
              .upload(`exports/${finalName}`, fileBuffer, {
                upsert: true,
                contentType: "text/csv"
              });

            console.log("🧹 Truncating inventory...");
            await supabase.rpc("truncate_inventory");

            console.log("📥 Importing CSV...");
            const rows = [];
            fs.createReadStream(finalName)
              .pipe(csv())
              .on("data", r => rows.push(r))
              .on("end", async () => {
                for (let i = 0; i < rows.length; i += 1000) {
                  await supabase
                    .from("inventory")
                    .insert(rows.slice(i, i + 1000));
                }

                console.log("✅ Import completed");
              });

          } catch (err) {
            console.error("❌ Supabase Error:", err.message);
          }

          // CLEANUP
          if (fs.existsSync(STATE_FILE)) fs.unlinkSync(STATE_FILE);
          if (fs.existsSync(WRITTEN_FILE)) fs.unlinkSync(WRITTEN_FILE);

          currentPage = 1;
          emptyPageCount = 0;
          totalSkipped = 0;
          workerStats = {};
          warehouseStats = {};
          written.clear();

          console.clear();
          console.log("🧹 Cleanup done.");
          console.log("🕒 Waiting for /start");
        });
      }, 2000);

      return;
    }

    currentPage++;
    spawnWorker(id, currentPage);
  });
}

// ================= API =================
app.get("/start", async (_, res) => {
  if (allowWorkers) return res.json({ status: "already running" });

  allowWorkers = true;
  jobFinished = false;

  currentPage = 1;
  emptyPageCount = 0;
  totalSkipped = 0;
  workerStats = {};
  warehouseStats = {};
  written.clear();

  stream = fs.createWriteStream(OUTPUT, { flags: "a" });
  headerWritten = false;

  await refreshToken();

  for (let i = 1; i <= WORKERS; i++) {
    spawnWorker(i, currentPage++);
  }

  res.json({ status: "started" });
});

app.listen(3000, () =>
  console.log("🚀 Server running at http://localhost:3000")
);
