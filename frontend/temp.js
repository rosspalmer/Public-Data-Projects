// new file to hold some of the initial test functions I built out. Want them out of the way, but will probably need them later.

const exampleData = {
  labels: [
    "2025-04-01", // Example dates
    "2025-04-02",
    "2025-04-03",
    "2025-04-04",
    "2025-04-05",
    "2025-04-06",
    "2025-04-07",
  ],
  temperatures: [
    68, // Average temperature for April 1st
    70, // Average temperature for April 2nd
    72, // Average temperature for April 3rd
    65, // Average temperature for April 4th
    74, // Average temperature for April 5th
    71, // Average temperature for April 6th
    69, // Average temperature for April 7th
  ],
};

// Function to display data fields
function displayDataFields() {
  const container = document.getElementById("data-container");
  Object.keys(dataFields).forEach((key) => {
    const div = document.createElement("div");
    div.innerText = `${key}: ${dataFields[key]}`;
    container.appendChild(div);
  });
}

// Initialize chart
function initializeChart() {
  const ctx = document.getElementById("tempChart").getContext("2d");
  const tempChart = new Chart(ctx, {
    type: "line",
    data: {
      // labels: [],
      // testing example
      labels: exampleData.labels,
      datasets: [
        {
          label: "Temperature Data",
          //data: [],
          //   testing example
          data: exampleData.temperatures,
          borderColor: "rgba(75, 192, 192, 1)",
          borderWidth: 1,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        y: {
          beginAtZero: true,
        },
      },
    },
  });
}

// document.addEventListener("DOMContentLoaded", () => {
//   initializeComponents();
// });

// function initializeComponents() {
//   //   initializeChart(); // Call the chart initialization
//   // displayMap(); // Call the map display function
//   //   displayDataFields(); // Optionally, show data fields
// }
