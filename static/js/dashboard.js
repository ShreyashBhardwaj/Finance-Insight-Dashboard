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
        if (!data.company_data) {
            console.error('No company data available');
            return;
        }

        const companyHeader = document.querySelector('.compnayname');
        const marketCap = document.querySelector('.marketcap');
        const forwardPE = document.querySelector('.forwardPE');
        const trailingPE = document.querySelector('.trailingPE');
        const dayLow = document.querySelector('.daylow');
        const dayHigh = document.querySelector('.dayhigh');

        companyHeader.textContent = data.company_data.name || 'N/A';
        marketCap.textContent = `Market Cap: ${data.company_data.market_cap || 'N/A'}`;
        forwardPE.textContent = `Forward P/E: ${data.company_data.forward_pe || 'N/A'}`;
        trailingPE.textContent = `Trailing P/E: ${data.company_data.trailing_pe || 'N/A'}`;
        dayLow.textContent = `Day Low: ${data.company_data.day_low || 'N/A'}`;
        dayHigh.textContent = `Day High: ${data.company_data.day_high || 'N/A'}`;

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
            p.textContent = `Quarter: ${item.fiscal_period}, Revenue: ${item.total_revenue}, Profit: ${item.net_income}`;
            quarterlyResultsContainer.appendChild(p);
        });

        // Populate income statement
        const incomeStatementContainer = document.querySelector('.oncomestmt');
        incomeStatementContainer.innerHTML = ''; // Clear existing data
        data.income_statement.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Year: ${item.fiscal_period}, Income: ${item.total_revenue}, Expenses: ${item.total_expenses}`;
            incomeStatementContainer.appendChild(p);
        });

        // Populate balance sheet
        const balanceSheetContainer = document.querySelector('.balancesheet');
        balanceSheetContainer.innerHTML = ''; // Clear existing data
        data.balance_sheet.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Year: ${item.fiscal_period}, Assets: ${item.total_assets}, Liabilities: ${item.total_liabilities}`;
            balanceSheetContainer.appendChild(p);
        });

        // Populate cash flow
        const cashFlowContainer = document.querySelector('.cashflow');
        cashFlowContainer.innerHTML = ''; // Clear existing data
        data.cash_flow.forEach(item => {
            const p = document.createElement('p');
            p.textContent = `Year: ${item.fiscal_period}, Operating Cash Flow: ${item.operating_cash_flow}, Free Cash Flow: ${item.free_cash_flow}`;
            cashFlowContainer.appendChild(p);
        });
    }
});
