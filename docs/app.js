// Global variables
const API_ENDPOINT = 'https://github.com/AKT8/Market_Data/releases/download/data-latest/data.duckdb'; // Replace with your actual endpoint
let screenerData = []; // Stores the fetched data

// Fetch data from the backend (data.duckdb)
async function fetchData() {
  try {
    // Fetch data from the API
    const response = await fetch(API_ENDPOINT);
    if (!response.ok) {
      throw new Error(`Error fetching data: ${response.statusText}`);
    }
    const data = await response.json();

    // Store the data globally and populate the table
    screenerData = data;
    populateTable(data);
  } catch (error) {
    console.error('Error fetching data:', error);
    alert('Failed to fetch data. Please try again later.');
  }
}

// Populate the screener table with data
function populateTable(data) {
  const tableBody = document.querySelector('#screener-table tbody');
  tableBody.innerHTML = ''; // Clear existing data

  data.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.Symbol}</td>
      <td>${row.Date}</td>
      <td>${row.Open.toFixed(2)}</td>
      <td>${row.High.toFixed(2)}</td>
      <td>${row.Low.toFixed(2)}</td>
      <td>${row.Close.toFixed(2)}</td>
    `;
    tableBody.appendChild(tr);
  });
}

// Apply filters to the screener table
function applyFilters() {
  const symbolFilter = document.getElementById('symbol-filter').value.toUpperCase();
  const dateFilter = document.getElementById('date-filter').value;

  // Filter the data based on user input
  const filteredData = screenerData.filter(row => {
    const symbolMatch = symbolFilter ? row.Symbol.toUpperCase().includes(symbolFilter) : true;
    const dateMatch = dateFilter ? row.Date === dateFilter : true;
    return symbolMatch && dateMatch;
  });

  // Update the table with filtered data
  populateTable(filteredData);
}

// Event Listeners
document.addEventListener('DOMContentLoaded', () => {
  // Fetch data when the page loads
  document.getElementById('fetch-data').addEventListener('click', fetchData);

  // Apply filters when the "Apply Filters" button is clicked
  document.getElementById('apply-filters').addEventListener('click', applyFilters);
});
