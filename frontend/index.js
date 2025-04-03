// const dataFields = {
//   TAVG: "average_daily_temperature",
//   TMAX: "average_daily_max_temperature",
//   TMIN: "average_daily_min_temperature",
//   ADPT: "average_dew_point_temperature",
//   // Continue with the other data fields...
// };

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

function displayMap() {
  var map = L.map("map").setView([37.7749, -122.4194], 5); // Default view centered on US
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
  }).addTo(map);

  document
    .getElementById("showMapButton")
    .addEventListener("click", function () {
      var zipCode = document.getElementById("zipInput").value;
      var apiUrl = `https://nominatim.openstreetmap.org/search?postalcode=${zipCode}&country=USA&format=json`;

      fetch(apiUrl)
        .then((response) => response.json())
        .then((data) => {
          if (data.length > 0) {
            var location = data[0];
            var lat = location.lat;
            var lng = location.lon;

            // Clear the map and set the view
            map.setView([lat, lng], 13);
            L.marker([lat, lng])
              .addTo(map)
              .bindPopup(`Zip Code: ${zipCode}`)
              .openPopup();
          } else {
            alert("Location not found");
          }
        })
        .catch((err) => {
          alert("Error fetching data");
        });
    });
}

document.addEventListener("DOMContentLoaded", () => {
  initializeComponents();
});

function initializeComponents() {
//   initializeChart(); // Call the chart initialization
  displayMap(); // Call the map display function
//   displayDataFields(); // Optionally, show data fields
}
