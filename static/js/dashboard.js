document.addEventListener('DOMContentLoaded', () => {
    const urlParams = new URLSearchParams(window.location.search);
    const companyName = urlParams.get('company');

    if (companyName) {
        fetchCompanyData(companyName);
    }

    async function fetchCompanyData(companyName) {
        try {
            const response = await fetch(`/dashboard?company=${encodeURIComponent(companyName)}`);
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const data = await response.json();
            populateDashboard(data);
        } catch (error) {
            console.error('Error fetching company data:', error);
        }
    }

    function populateDashboard(data) {
        const companyHeader = document.querySelector('.compnayname');
        const marketCap = document.querySelector('.marketcap');
        const forwardPE = document.querySelector('.forwardPE');
        const trailingPE = document.querySelector('.trailingPE');
        const dayLow = document.querySelector('.daylow');
        const dayHigh = document.querySelector('.dayhigh');

        companyHeader.textContent = data.company_data.name;
        marketCap.textContent = `Market Cap: ${data.company_data.market_cap}`;
        forwardPE.textContent = `Forward P/E: ${data.company_data.forward_pe}`;
        trailingPE.textContent = `Trailing P/E: ${data.company_data.trailing_pe}`;
        dayLow.textContent = `Day Low: ${data.company_data.day_low}`;
        dayHigh.textContent = `Day High: ${data.company_data.day_high}`;

        // Populate historical data
        const historicalDataContainer = document.querySelector('.historicaldata');
        historicalDataContainer.innerHTML = ''; // Clear existing data
        data.historical_data.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Date: ${item.date}, Close Price: ${item.close_price}`;
            historicalDataContainer.appendChild(p);
        });

        // Populate quarterly results
        const quarterlyResultsContainer = document.querySelector('.quarterlyresult');
        quarterlyResultsContainer.innerHTML = ''; // Clear existing data
        data.quarterly_results.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Quarter: ${item.quarter}, Revenue: ${item.revenue}, Profit: ${item.profit}`;
            quarterlyResultsContainer.appendChild(p);
        });

        // Populate income statement
        const incomeStatementContainer = document.querySelector('.oncomestmt');
        incomeStatementContainer.innerHTML = ''; // Clear existing data
        data.income_statement.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Year: ${item.year}, Income: ${item.income}, Expenses: ${item.expenses}`;
            incomeStatementContainer.appendChild(p);
        });

        // Populate balance sheet
        const balanceSheetContainer = document.querySelector('.balancesheet');
        balanceSheetContainer.innerHTML = ''; // Clear existing data
        data.balance_sheet.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Year: ${item.year}, Assets: ${item.assets}, Liabilities: ${item.liabilities}`;
            balanceSheetContainer.appendChild(p);
        });

        // Populate cash flow
        const cashFlowContainer = document.querySelector('.cashflow');
        cashFlowContainer.innerHTML = ''; // Clear existing data
        data.cash_flow.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Year: ${item.year}, Operating Cash Flow: ${item.operating_cash_flow}, Free Cash Flow: ${item.free_cash_flow}`;
            cashFlowContainer.appendChild(p);
        });
    }
});
