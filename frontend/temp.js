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


// GLOBAL VARIABLES AND TEST DATA
let lat;
let lng;
let map;
let stationMarkers = [];
let stationMap;

const exampleStations = {
  stations: [
    {
      ghcn_id: "USC00051528",
      lat: 39.2203,
      long: -105.2783,
      distance_mil: 0.23,
      elevation_m: 2095,
      name: "CHEESMAN",
      city: "Bailey",
      county: "Park County",
      state: "Colorado",
    },
    {
      ghcn_id: "USC00053005",
      lat: 40.5764,
      long: -105.0858,
      distance_mil: 120.0,
      elevation_m: 1525,
      name: "FT COLLINS",
      city: "Fort Collins",
      county: "Park County",
      state: "Colorado",
    },
    {
      ghcn_id: "USC00051528",
      lat: 37.1997,
      long: -108.4892,
      distance_mil: 230.0,
      elevation_m: 2176,
      name: "MESA VERDE NP",
      city: "Cortez",
      county: "Park County",
      state: "Colorado",
    },
  ],
};

function displayMap() {
  if (!map) {
    map = L.map("stationMap").setView([37.7749, -122.4194], 5);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
    }).addTo(map);
  }

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

function convertZipCode() {
  return new Promise((resolve, reject) => {
    // Return a promise
    var zipCode = document.getElementById("zipInput").value;
    // console.log(zipCode);
    var apiUrl = `https://nominatim.openstreetmap.org/search?postalcode=${zipCode}&country=USA&format=json`;

    fetch(apiUrl)
      .then((response) => response.json())
      .then((data) => {
        if (data.length > 0) {
          var location = data[0];
          var lat = location.lat;
          var lng = location.lon;
          resolve({ lat, lng }); // Resolve with lat and lng
        } else {
          alert("Location not found");
          reject("Location not found"); // Reject with error message
        }
      })
      .catch((err) => {
        alert("Error fetching data");
        reject(err); // Reject on error
      });
  });
}

function getStationData() {
  convertZipCode()
    .then(({ lat, lng }) => {
      console.log(lat, lng);

      // Show popup
      const popup = document.getElementById("stationsPopup");
      popup.style.display = "flex";

      // Clear old content if exists
      if(document.getElementById("stationCards") != null){
        const cardContainer = document.getElementById("stationCards");
        cardContainer.innerHTML = "";
      }

      // Build station cards
      exampleStations.stations.forEach((station) => {
        const card = document.createElement("div");
        card.className = "station-card";

        const title = document.createElement("div");
        title.className = "station-title";

        const nameSpan = document.createElement("span");
        nameSpan.textContent = station.name;

        const distSpan = document.createElement("span");
        distSpan.textContent = `${station.distance_mil.toFixed(1)} mi`;

        title.appendChild(nameSpan);
        title.appendChild(distSpan);
        card.appendChild(title);

        const details = document.createElement("div");
        details.className = "station-details";

        const createP = (label, value) => {
          const p = document.createElement("p");
          p.innerHTML = `<strong>${label}:</strong> ${value}`;
          return p;
        };

        // Example data, some of this (distance) will probably need to be calculated differently
        details.appendChild(
          createP("Location", `${station.city}, ${station.state}`)
        );
        details.appendChild(createP("County", station.county));
        details.appendChild(createP("Elevation", `${station.elevation_m} m`));
        details.appendChild(
          createP("Coordinates", `${station.lat}, ${station.long}`)
        );
        details.appendChild(createP("Station ID", station.ghcn_id));

        card.appendChild(details);
        cardContainer.appendChild(card);

      });
    })
    .catch((err) => {
      console.error(err);
    });
}
// function getStationData() {
//   convertZipCode()
//     .then(({ lat, lng }) => {
//       // Make sure the map container is visible and has size
//       const mapDiv = document.getElementById("stationMap");
//       mapDiv.style.display = "block";

//       initializeMap(); // Initialize once if needed

//       // Now, set view or fit bounds
//       stationMap.setView([lat, lng], 13);

//       // Clear existing markers if any
//       stationMap.eachLayer((layer) => {
//         if (layer instanceof L.Marker) {
//           stationMap.removeLayer(layer);
//         }
//       });

//       const bounds = L.latLngBounds();

//       // Add station markers
//       exampleStations.stations.forEach((station) => {
//         const marker = L.marker([station.lat, station.long])
//           .addTo(stationMap)
//           .bindPopup(`<strong>${station.name}</strong><br>${station.city}, ${station.state}`);
//         bounds.extend([station.lat, station.long]);
//       });

//       // Fit to bounds of stations
//       if (exampleStations.stations.length > 0) {
//         stationMap.fitBounds(bounds);
//       }

//       // If the map container was hidden before, sometimes Leaflet needs to invalidate size
//       stationMap.invalidateSize();
//     })
//     .catch((err) => {
//       console.error(err);
//     });
// }
// Close button



function closePopup() {
  document.getElementById("stationsPopup").style.display = "none";
}
