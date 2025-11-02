// Global variables
const DUCKDB_URL = 'https://github.com/AKT8/Market_Data/releases/download/data-latest/data.duckdb';
let screenerData = []; // Stores the fetched data

// Load DuckDB-WASM and query the file
async function fetchData() {
  try {
    // Dynamically import DuckDB-WASM from CDN
    const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-esm.js');

    // Select the appropriate bundle for your browser
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    // Start the DuckDB worker
    const worker = new Worker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    // Fetch the .duckdb file from GitHub Releases
    const response = await fetch(DUCKDB_URL);
    if (!response.ok) throw new Error(`Error fetching file: ${response.statusText}`);

    const arrayBuffer = await response.arrayBuffer();
    const uint8Array = new Uint8Array(arrayBuffer);

    // Register and attach the database
    await db.registerFileBuffer('data.duckdb', uint8Array);
    const conn = await db.connect();
    await conn.query("ATTACH 'data.duckdb' AS data;");

    // Query the database
    // Replace `my_table` with your actual table name inside the DuckDB file
    const result = await conn.query("SELECT * FROM data.my_table LIMIT 500;");
    const rows = result.toArray().map(Object.fromEntries);

    // Store and populate table
    screenerData = rows;
    populateTable(rows);
    alert("Data loaded successfully!");

  } catch (error) {
    console.error('Error loading DuckDB data:', error);
    alert('Failed to load data. See console for details.');
  }
}

// Populate the screener table
function populateTable(data) {
  const tableBody = document.querySelector('#screener-table tbody');
  tableBody.innerHTML = '';

  data.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.Symbol || ''}</td>
      <td>${row.Date || ''}</td>
      <td>${Number(row.Open || 0).toFixed(2)}</td>
      <td>${Number(row.High || 0).toFixed(2)}</td>
      <td>${Number(row.Low || 0).toFixed(2)}</td>
      <td>${Number(row.Close || 0).toFixed(2)}</td>
    `;
    tableBody.appendChild(tr);
  });
}

// Apply filters
function applyFilters() {
  const symbolFilter = document.getElementById('symbol-filter').value.toUpperCase();
  const dateFilter = document.getElementById('date-filter').value;

  const filteredData = screenerData.filter(row => {
    const symbolMatch = symbolFilter ? (row.Symbol || '').toUpperCase().includes(symbolFilter) : true;
    const dateMatch = dateFilter ? row.Date === dateFilter : true;
    return symbolMatch && dateMatch;
  });

  populateTable(filteredData);
}

// Event Listeners
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('fetch-data').addEventListener('click', fetchData);
  document.getElementById('apply-filters').addEventListener('click', applyFilters);
});
