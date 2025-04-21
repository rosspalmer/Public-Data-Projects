// GLOBAL VARIABLES AND TEST DATA 
let lat;
let lng;

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

    // Clear old content
    const cardContainer = document.getElementById("stationCards");
    cardContainer.innerHTML = "";

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
      details.appendChild(createP("Location", `${station.city}, ${station.state}`));
      details.appendChild(createP("County", station.county));
      details.appendChild(createP("Elevation", `${station.elevation_m} m`));
      details.appendChild(createP("Coordinates", `${station.lat}, ${station.long}`));
      details.appendChild(createP("Station ID", station.ghcn_id));

      card.appendChild(details);
      cardContainer.appendChild(card);

      // Then we will be able to click on a specific station and pull up the map??
    });
  })
  .catch((err) => {
    console.error(err);
  });
}

// Close button
function closePopup(){
  document.getElementById("stationsPopup").style.display = "none";
}
