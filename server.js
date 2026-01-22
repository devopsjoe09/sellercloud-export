import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.send("SellerCloud Export API is running");
});

app.post("/run", async (req, res) => {
  try {
    // TODO: call your worker logic here
    res.json({ status: "ok", message: "Job started" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
