// script.js

document.addEventListener("DOMContentLoaded", () => {
    const darkModeButton = document.getElementById("dark-mode");
    const lightModeButton = document.getElementById("light-mode");
    const autoModeButton = document.getElementById("auto-mode");

    // Load user's preference from localStorage
    const userPreference = localStorage.getItem("theme");
    if (userPreference) {
        document.body.classList.toggle(userPreference);
    }

    darkModeButton.addEventListener("click", () => {
        setTheme("dark-mode");
    });

    lightModeButton.addEventListener("click", () => {
        setTheme("light-mode");
    });

    autoModeButton.addEventListener("click", () => {
        setTheme(null);
    });

    function setTheme(mode) {
        if (mode) {
            document.body.className = mode;
            localStorage.setItem("theme", mode);
        } else {
            document.body.className = '';
            localStorage.removeItem("theme");
        }
    }
});
