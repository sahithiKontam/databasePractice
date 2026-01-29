const axios = require("axios");
const pool = require("./db");

const BASE_URL =
  "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL";

const PER_PAGE = 100;
const MAX_PAGES = 173;
const CONCURRENT_REQUESTS = 7;

let totalFetchTime = 0n;
let totalInsertTime = 0n;

console.time("Total Execution Time");

async function fetchPage(page) {
  const start = process.hrtime.bigint();

  const response = await axios.get(BASE_URL, {
    params: {
      format: "json",
      per_page: PER_PAGE,
      page,
    },
    timeout: 15000,
  });

  const end = process.hrtime.bigint();
  totalFetchTime += end - start;

  return response.data[1] || [];
}
//bulk insertion
async function bulkInsert(records) {
  const values = [];
  const placeholders = [];
  let rowIndex = 0;

  for (const r of records) {
    if (!r.countryiso3code || !r.date) continue;

    const base = rowIndex * 4;
    placeholders.push(
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`
    );

    values.push(
      r.countryiso3code,
      r.country?.value || null,
      parseInt(r.date),
      r.value
    );

    rowIndex++;
  }

  if (values.length === 0) return;

  const start = process.hrtime.bigint();

  await pool.query(
    `
    INSERT INTO world_bank_population
    (country_code, country_name, year, population)
    VALUES ${placeholders.join(",")}
    ON CONFLICT (country_code, year)
    DO UPDATE SET population = EXCLUDED.population
    `,
    values
  );

  const end = process.hrtime.bigint();
  totalInsertTime += end - start;
}

async function fetchAndSave() {
  for (let start = 1; start <= MAX_PAGES; start += CONCURRENT_REQUESTS) {
    const pages = [];

    for (let p = start; p < start + CONCURRENT_REQUESTS && p <= MAX_PAGES; p++) {
      pages.push(p);
    }

    console.log(`Fetching pages ${pages.join(", ")}`);

    const pageResults = await Promise.all(
      pages.map((p) =>
        fetchPage(p).catch(() => {
          console.error(`Page ${p} failed`);
          return [];
        })
      )
    );

    const allRecords = pageResults.flat();
    await bulkInsert(allRecords);

    console.log(`Saved pages ${pages.join(", ")}`);
  }

  // timing logs
  console.log(
    `Total Fetch Time: ${(Number(totalFetchTime) / 1e9).toFixed(2)} seconds`
  );
  console.log(
    `Total Insert Time: ${(Number(totalInsertTime) / 1e9).toFixed(2)} seconds`
  );

  console.timeEnd("Total Execution Time");
  console.log("DATA FETCH & SAVE COMPLETED");

  process.exit(0);
}

// ---------- Run ----------
fetchAndSave().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

//separte exe time for insertionnor fetching
//optimzation
//postgres connections any other connncetions