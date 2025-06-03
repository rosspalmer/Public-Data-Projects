// GLOBAL VARIABLES AND TEST DATA
let lat;
let lng;
let map;
let stationMarkers = [];
let stationMap;
let markersLayer;

const stations = [
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
];

function initMap() {
  // Initialize map once
  if (!map) {
    map = L.map("map").setView([39, -105], 6);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
    }).addTo(map);
    // Layer for markers
    markersLayer = L.layerGroup().addTo(map);
  }
}

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

function loadStations() {
  // Populate list
  const listDiv = document.getElementById("list");
  listDiv.innerHTML = ""; // clear existing
  initMap(); // ensure map initialized

  // Clear markers
  markersLayer.clearLayers();

  const createP = (label, value) => {
    const p = document.createElement("p");
    p.innerHTML = `<strong>${label}:</strong> ${value}`;
    return p;
  };

  // Add stations to list and map
  stations.forEach((station) => {
    // Add card to list
    const card = document.createElement("div");
    card.className = "station-card";
    card.innerHTML = `<strong>${station.name}</strong><br>
                      ${station.city}, ${station.state}`;
    card.appendChild(createP("Location", `${station.city}, ${station.state}`));
    card.appendChild(createP("County", station.county));
    card.appendChild(createP("Elevation", `${station.elevation_m} m`));
    card.appendChild(createP("Coordinates", `${station.lat}, ${station.long}`));
    card.appendChild(createP("Station ID", station.ghcn_id));
    listDiv.appendChild(card);

    // Add marker to map
    const marker = L.marker([station.lat, station.long])
      .addTo(markersLayer)
      .bindPopup(
        `<strong>${station.name}</strong><br>${station.city}, ${station.state}`
      );
    // Optional: open popup on hover or click
  });

  // Fit map bounds
  const bounds = L.latLngBounds(stations.map((s) => [s.lat, s.long]));
  if (stations.length > 0) {
    map.fitBounds(bounds);
  }
}

function closePopup() {
  document.getElementById("stationsPopup").style.display = "none";
}
