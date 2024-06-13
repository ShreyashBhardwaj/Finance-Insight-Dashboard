document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.querySelector('.search-bar input');
    const searchButton = document.querySelector('.search-bar button');
    const suggestionsContainer = document.querySelector('.suggestions');

    searchButton.addEventListener('click', () => {
        const query = searchInput.value.trim(); // Trim whitespace from input
        if (query) {
            fetchCompanies(query);
        } else {
            suggestionsContainer.innerHTML = ''; // Clear suggestions if input is empty
        }
    });

    searchInput.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            const query = searchInput.value.trim(); // Trim whitespace from input
            if (query) {
                fetchCompanies(query);
            } else {
                suggestionsContainer.innerHTML = ''; // Clear suggestions if input is empty
            }
        }
    });

    async function fetchCompanies(query) {
        try {
            const response = await fetch(`/search?q=${encodeURIComponent(query)}`);
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const companies = await response.json();
            displaySuggestions(companies);
        } catch (error) {
            console.error('Error fetching companies:', error);
        }
    }

    function displaySuggestions(companies) {
        suggestionsContainer.innerHTML = '';
        companies.forEach(company => {
            const button = document.createElement('button');
            button.textContent = company.name;
            button.addEventListener('click', () => {
                navigateToDashboard(company.name);
            });
            suggestionsContainer.appendChild(button);
        });
    }

    function navigateToDashboard(companyName) {
        // Redirect to Flask route for dashboard with company name as query parameter
        window.location.href = `/dashboard?company=${encodeURIComponent(companyName)}`;
    }
});
