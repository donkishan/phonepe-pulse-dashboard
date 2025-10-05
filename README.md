

📊 PhonePe Pulse Dashboard

An interactive Streamlit dashboard to explore PhonePe Pulse data across India, visualizing transactions, user engagement, and insurance insights. The project fetches data from a MySQL database populated from the official PhonePe Pulse JSON datasets.

🔹 Features

Home Dashboard

3D Choropleth Map of India with state-wise metrics

Overview Metrics: Total Transaction/Insurance Value, Count, Registered Users

State-wise bar charts for quick comparison

Analysis Page (Case Studies)

Transaction Trends: Total transactions & amounts with YoY growth

Device Dominance: User engagement by device brands

Insurance Analysis: Policy count & premium amount trends

Payment Category Performance: Pie and bar charts

User Engagement Metrics: App opens, opens per user, scatter plots

Dynamic filters for Year, Quarter, and State

Fully interactive charts using Plotly and PyDeck for 3D maps

🔹 Installation
1. Clone the repository
git clone https://github.com/<your-username>/phonepe-pulse-dashboard.git
cd phonepe-pulse-dashboard

2. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

3. Install dependencies
pip install -r requirements.txt


Dependencies include: streamlit, pandas, mysql-connector-python, plotly, pydeck, requests, python-dotenv

🔹 Database Setup

Install MySQL and create a database:

CREATE DATABASE phonepe_pulse;


Run phonepe_loader.py to populate tables from the official PhonePe Pulse JSON datasets:

python phonepe_loader.py


Tables created:

aggregated_transaction

aggregated_user

aggregated_insurance

map_transaction

map_user

map_insurance

top_transaction

top_user

top_insurance

🔹 Configuration

Create a .env file in the root directory:

DB_HOST=localhost
DB_USER=root
DB_PASSWORD=<your_mysql_password>
DB_NAME=phonepe_pulse

🔹 Run the App
streamlit run app.py


Open your browser at http://localhost:8501

Use the sidebar to navigate between Home and Analysis pages

🔹 Project Structure
phonepe-pulse-dashboard/
│
├─ app.py                  # Streamlit dashboard
├─ phonepe_loader.py       # Loader script for MySQL database
├─ requirements.txt        # Python dependencies
├─ .env                    # Database credentials
└─ README.md               # Project documentation

🔹 Screenshots

Home Page 3D Map

Analysis Page Example

🔹 Contributing

Fork the repository

Create a new branch: git checkout -b feature-name

Commit your changes: git commit -m "Description"

Push to branch: git push origin feature-name

Create a Pull Request

🔹 License

MIT License © [Kishan H S]