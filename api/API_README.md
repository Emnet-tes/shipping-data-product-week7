# Telegram Analytics API

A comprehensive FastAPI-based analytical API for Telegram channel data with YOLO object detection insights.

## 🚀 Features

- **Real-time Analytics**: Query telegram message data with engagement metrics
- **Object Detection**: YOLO-powered image analysis and object detection results
- **Channel Analytics**: Comprehensive channel performance metrics
- **Message Search**: Full-text search across message content
- **Data Validation**: Pydantic schemas for request/response validation
- **Auto Documentation**: OpenAPI/Swagger documentation at `/docs`

## 📋 Requirements

- Python 3.8+
- PostgreSQL database with analytics schema
- Required packages: `fastapi`, `uvicorn`, `psycopg2-binary`, `pydantic`

## 🛠️ Installation

1. Install dependencies:

```bash
pip install fastapi uvicorn psycopg2-binary pydantic python-multipart
```

2. Configure database connection in `api/database.py` or set environment variables:

```bash
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_NAME=kara_medical
export DB_USER=postgres
export DB_PASSWORD=your_secure_password
```

3. Test the setup:

```bash
python scripts\test_api.py
```

## 🏃 Running the API

### Method 1: Using the convenience script (Recommended)

```bash
python scripts\start_api.py
```

### Method 2: Using uvicorn directly

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Method 3: Using python module

```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

## 📚 API Documentation

Once the server is running, visit:

- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## 🔗 API Endpoints

### Core Analytics Endpoints

#### 1. Top Products/Objects

```
GET /api/reports/top-products?limit=10
```

Returns the most frequently detected objects/products from images.

**Parameters:**

- `limit` (optional): Number of results (1-100, default: 10)

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "object_class": "person",
      "object_category": "people",
      "total_detections": 207,
      "messages_with_object": 45,
      "channels_with_object": 3,
      "avg_confidence": 0.85,
      "frequency_category": "very_common",
      "importance_score": 176.0
    }
  ],
  "total_records": 10,
  "limit": 10
}
```

#### 2. Channel Activity

```
GET /api/channels/{channel_name}/activity?days=30
```

Returns posting activity for a specific channel over time.

**Parameters:**

- `channel_name` (required): Channel name
- `days` (optional): Days to look back (1-365, default: 30)

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "channel_name": "tikvahpharma",
      "date": "2024-01-15T00:00:00",
      "message_count": 5,
      "total_views": 1250,
      "total_forwards": 23,
      "avg_engagement_score": 0.75
    }
  ],
  "total_records": 30
}
```

#### 3. Message Search

```
GET /api/search/messages?query=paracetamol&limit=50
```

Searches for messages containing specific keywords.

**Parameters:**

- `query` (required): Search term
- `limit` (optional): Max results (1-200, default: 50)

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "message_id": 123,
      "channel_name": "tikvahpharma",
      "message_date": "2024-01-15T10:30:00",
      "view_count": 250,
      "forward_count": 5,
      "reply_count": 2,
      "engagement_score": 0.85,
      "reach_category": "high",
      "message_text": "New paracetamol stock available...",
      "has_media": true
    }
  ],
  "total_records": 15,
  "query": "paracetamol"
}
```

### Additional Endpoints

#### 4. Channel Analytics

```
GET /api/channels/{channel_name}/analytics
```

Comprehensive analytics for a specific channel.

#### 5. Top Channels

```
GET /api/channels?limit=10
```

List of channels ordered by activity.

#### 6. Engagement Metrics

```
GET /api/engagement/metrics?days=30
```

Engagement metrics over time.

#### 7. Object Detections

```
GET /api/detections?object_class=person&confidence_level=high&limit=50
```

Object detection results with filters.

#### 8. Channel List

```
GET /api/channels/list
```

Simple list of all channels.

#### 9. Summary Statistics

```
GET /api/stats/summary
```

Overall dataset statistics.

#### 10. Health Check

```
GET /health
```

API health status and database connectivity.

## 🧪 Testing

### Test API Components

```bash
python scripts\test_api.py
```

### Test All Endpoints

```bash
# Start the server first
python scripts\start_api.py

# In another terminal
python scripts\test_endpoints.py
```

## 📊 Data Models

The API uses the following dbt models:

- `analytics.fct_messages` - Message fact table
- `analytics.fct_image_detections` - Image detection fact table
- `analytics.dim_channels` - Channel dimension
- `analytics.dim_dates` - Date dimension
- `analytics.dim_objects` - Object/product dimension

## 🔧 Configuration

### Database Configuration

Update `api/database.py` or set environment variables:

- `DB_HOST`: Database host (default: 127.0.0.1)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: kara_medical)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password

### API Configuration

The API includes:

- CORS middleware for cross-origin requests
- Request validation with Pydantic
- Error handling and logging
- Connection pooling
- Rate limiting ready

## 🚨 Error Handling

The API includes comprehensive error handling:

- HTTP 400: Bad Request (validation errors)
- HTTP 404: Not Found (missing resources)
- HTTP 500: Internal Server Error (database/system errors)

All errors return structured responses:

```json
{
  "status": "error",
  "message": "Error description",
  "error_code": "ERROR_CODE",
  "timestamp": "2024-01-15T10:30:00"
}
```

## 📈 Performance

- Connection pooling for database efficiency
- Indexed queries for fast response times
- Pagination support for large datasets
- Response time logging
- Memory-efficient data processing

## 🔐 Security

- Input validation with Pydantic
- SQL injection prevention with parameterized queries
- CORS configuration
- Environment variable configuration
- Database connection security

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License.
