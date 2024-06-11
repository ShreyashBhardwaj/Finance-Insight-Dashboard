document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.querySelector('.search-bar input');
    const searchButton = document.querySelector('.search-bar button');
    const suggestionsContainer = document.querySelector('.suggestions');

    searchButton.addEventListener('click', () => {
        const query = searchInput.value;
        if (query) {
            fetchCompanies(query);
        }
    });

    searchInput.addEventListener('keypress', (event) => {
        if (event.key === 'Enter') {
            const query = searchInput.value;
            if (query) {
                fetchCompanies(query);
            }
        }
    });

    async function fetchCompanies(query) {
        try {
            const response = await fetch(`http://localhost:5000/search?q=${encodeURIComponent(query)}`);
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
            suggestionsContainer.appendChild(button);
        });
    }
});
