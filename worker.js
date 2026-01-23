const axios = require("axios");
const { parentPort, workerData } = require("worker_threads");

const ALLOWED_WAREHOUSES = [
  "Moorpark Logistics",
  "AM NJ 3PL"
];

async function run() {
  const { BASE_URL, TOKEN, PAGE_SIZE, page } = workerData;

  try {
    const res = await axios.get(BASE_URL, {
      headers: { Authorization: `Bearer ${TOKEN}` },
      params: {
        pageNumber: page,
        pageSize: PAGE_SIZE
      }
    });

    const items = res.data?.Items || [];

    const filtered = items.filter(i =>
      i.ActiveStatus === "Active" &&
      ALLOWED_WAREHOUSES.includes(
        String(i.WarehouseName || "").trim()
      )
    );

    const warehouseCount = {};
    filtered.forEach(i => {
      const wh = String(i.WarehouseName).trim();
      warehouseCount[wh] = (warehouseCount[wh] || 0) + 1;
    });

    parentPort.postMessage({
      page,
      activeItems: filtered,
      skipped: items.length - filtered.length,
      warehouseCount
    });

  } catch (err) {
    if (err.response?.status === 401) {
      parentPort.postMessage({ type: "expired" });
    } else {
      parentPort.postMessage({
        page,
        activeItems: [],
        skipped: 0,
        warehouseCount: {}
      });
    }
  }
}

run();
