# DBStress

Oracle Database Stress Testing Tool with Real-time Monitoring

## Features

- **Complete Online Sales Schema**: Creates a fully-functional e-commerce database schema with products, customers, orders, inventory, payments, and more
- **Configurable Workload**: Adjust the number of concurrent sessions and the rate of INSERT, UPDATE, DELETE, and SELECT operations
- **Scalable Schema**: Set a scale factor to generate larger datasets for testing
- **Real-time Monitoring**: Live charts showing:
  - Transactions per Second (TPS)
  - DML Operations per Second (INSERT, UPDATE, DELETE)
  - Top 10 Wait Events from Oracle V$SYSTEM_EVENT
- **Live Configuration**: Modify workload parameters while the stress test is running

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     React Frontend                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ Connection  │  │   Schema    │  │   Stress Config     │ │
│  │   Panel     │  │   Panel     │  │      Panel          │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  TPS Chart  │  │ Operations  │  │   Wait Events       │ │
│  │             │  │   Chart     │  │      Table          │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────┬───────────────────────────────────┘
                          │ WebSocket (Socket.IO)
┌─────────────────────────┴───────────────────────────────────┐
│                    Node.js Backend                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Oracle    │  │   Stress    │  │     Metrics         │ │
│  │     DB      │  │   Engine    │  │    Collector        │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────┬───────────────────────────────────┘
                          │ oracledb
┌─────────────────────────┴───────────────────────────────────┐
│                   Oracle Database                           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Online Sales Schema                     │   │
│  │  Regions → Countries → Warehouses                   │   │
│  │  Categories → Products → Inventory                  │   │
│  │  Customers → Orders → Order Items → Payments        │   │
│  │  Product Reviews, Order History                     │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Node.js 18+
- Oracle Database (19c or later recommended)
- Oracle Instant Client (required for oracledb npm package)

### Installing Oracle Instant Client

1. Download Oracle Instant Client from [Oracle's website](https://www.oracle.com/database/technologies/instant-client/downloads.html)
2. Follow the installation instructions for your platform
3. Set environment variables:
   ```bash
   export LD_LIBRARY_PATH=/path/to/instantclient:$LD_LIBRARY_PATH
   # or on macOS
   export DYLD_LIBRARY_PATH=/path/to/instantclient:$DYLD_LIBRARY_PATH
   ```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd DBStress
   ```

2. Install dependencies:
   ```bash
   npm run install-all
   ```

3. Create environment file:
   ```bash
   cp .env.example .env
   # Edit .env with your Oracle connection details
   ```

## Running the Application

### Development Mode

Start both backend and frontend with hot-reloading:
```bash
npm run dev
```

Or run them separately:
```bash
# Terminal 1 - Backend
npm run server

# Terminal 2 - Frontend
npm run client
```

### Production Mode

1. Build the React frontend:
   ```bash
   npm run build
   ```

2. Start the server:
   ```bash
   npm start
   ```

Access the application at `http://localhost:3001`

## Usage

### 1. Connect to Database

Enter your Oracle database credentials:
- **Username**: Your Oracle database user
- **Password**: Your Oracle database password
- **Connection String**: Format `host:port/service_name` (e.g., `localhost:1521/ORCLPDB1`)

### 2. Create Schema

Choose a scale factor to determine the size of your test data:

| Scale Factor | Customers | Products | Orders | Est. Load Time |
|-------------|-----------|----------|--------|----------------|
| 1x          | 1,000     | 500      | 5,000  | ~10-20 sec     |
| 10x         | 10,000    | 5,000    | 50,000 | ~2-3 min       |
| 50x         | 50,000    | 25,000   | 250,000| ~8-12 min      |
| 100x        | 100,000   | 50,000   | 500,000| ~15-25 min     |

**Performance Optimizations**: The schema creation process has been optimized for faster loading:
- **Deferred Index Creation**: Indexes are created after data population to avoid overhead during bulk inserts
- **Bulk Operations**: Uses Oracle's `executeMany` for efficient batch processing
- **Three-Phase Progress**: Schema creation (0-30%), data population (30-90%), index creation (90-100%)

See [PERFORMANCE_IMPROVEMENTS.md](PERFORMANCE_IMPROVEMENTS.md) for detailed information about performance optimizations.

### 3. Configure and Run Stress Test

Adjust the workload parameters:

- **Concurrent Sessions**: Number of parallel database connections (1-100)
- **INSERTs/sec**: Target insert operations per second
- **UPDATEs/sec**: Target update operations per second
- **DELETEs/sec**: Target delete operations per second
- **SELECTs/sec**: Target select operations per second
- **Think Time**: Delay between operations (lower = more aggressive)

### 4. Monitor Performance

While the stress test is running, monitor:

- **Real-time TPS chart**: Shows transactions per second over time
- **Operations chart**: Shows INSERT, UPDATE, DELETE rates
- **Wait Events table**: Top 10 non-idle wait events from the database
- **Session statistics**: Active/inactive/blocked sessions

## Schema Details

The online sales schema includes:

| Table | Description |
|-------|-------------|
| `regions` | Geographic regions |
| `countries` | Countries with region mapping |
| `warehouses` | Distribution centers |
| `categories` | Product categories (hierarchical) |
| `products` | Product catalog |
| `inventory` | Stock levels per product per warehouse |
| `customers` | Customer information |
| `orders` | Sales orders |
| `order_items` | Order line items |
| `payments` | Payment transactions |
| `order_history` | Order status change history |
| `product_reviews` | Customer reviews |

## API Endpoints

### Database Connection
- `POST /api/db/test-connection` - Test database connection
- `POST /api/db/connect` - Connect to database
- `POST /api/db/disconnect` - Disconnect from database
- `GET /api/db/status` - Get connection status

### Schema Management
- `POST /api/schema/create` - Create schema with scale factor
- `POST /api/schema/drop` - Drop schema
- `GET /api/schema/info` - Get schema statistics

### Stress Test
- `POST /api/stress/start` - Start stress test
- `POST /api/stress/stop` - Stop stress test
- `GET /api/stress/status` - Get stress test status
- `PUT /api/stress/config` - Update configuration live

## Configuration

Environment variables (`.env`):

```env
# Oracle Database Connection
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
ORACLE_CONNECTION_STRING=localhost:1521/ORCLPDB1

# Server Configuration
PORT=3001
```

## Troubleshooting

### Common Issues

1. **Connection errors**: Ensure Oracle Instant Client is installed and environment variables are set
2. **V$ view access**: Some metrics require SELECT privileges on V$ views
3. **Pool exhaustion**: Reduce session count if you see connection pool errors

### Required Oracle Privileges

For full functionality, the database user needs:
```sql
GRANT CREATE SESSION TO your_user;
GRANT CREATE TABLE TO your_user;
GRANT CREATE SEQUENCE TO your_user;
GRANT UNLIMITED TABLESPACE TO your_user;

-- For monitoring (optional but recommended)
GRANT SELECT ON V_$SESSION TO your_user;
GRANT SELECT ON V_$SYSTEM_EVENT TO your_user;
GRANT SELECT ON V_$SYSSTAT TO your_user;
GRANT SELECT ON V_$SQL TO your_user;
```

## License

MIT
