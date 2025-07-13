# Ethiopian Medical Business Telegram Scraper

This project scrapes data from Ethiopian medical business Telegram channels for business intelligence and analysis.

## Features

### 📊 Data Collection
- **Message Scraping**: Collect text messages, metadata (views, forwards, replies)
- **Image Collection**: Download and organize images for object detection
- **User Information**: Capture sender details when available
- **Channel Metadata**: Store channel information and statistics

### 🗂️ Data Organization
- **Partitioned Structure**: Data organized by date (`YYYY-MM-DD`)
- **JSON Format**: Raw data preserved in JSON format
- **Media Organization**: Images stored separately by channel and date
- **Metadata Files**: Comprehensive metadata for each scraping session

### 📝 Logging & Monitoring
- **Comprehensive Logging**: Track successful/failed operations
- **Error Handling**: Robust error handling with detailed error logs
- **Progress Tracking**: Real-time progress updates
- **Summary Reports**: Detailed summary after each scraping session

## Directory Structure

```
data/
├── raw/
│   ├── telegram_messages/
│   │   └── YYYY-MM-DD/
│   │       ├── channel_name.json
│   │       ├── channel_name_metadata.json
│   │       └── scraping_summary.json
│   └── telegram_images/
│       └── YYYY-MM-DD/
│           └── channel_name/
│               ├── image1.jpg
│               └── image2.png
├── processed/
└── logs/
    └── telegram_scraper_YYYYMMDD.log
```

## Target Channels

### Verified Channels ✅
- `@lobelia4cosmetics` - Lobelia for Cosmetics
- `@tikvahpharma` - Tikvah Pharma
- `@CheMed123` - chemed


## Setup

### 1. Environment Setup
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\\Scripts\\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Telegram API Configuration
1. Go to https://my.telegram.org
2. Create a new app and get your API credentials
3. Create a `.env` file in the project root:

```env
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=yourpassword
PGDATABASE=kara_medical
```

### 3. Database Setup (Optional)
```bash
# Start PostgreSQL with Docker
docker-compose up -d db
```

## Usage

### Basic Scraping
```bash
# Run the main scraper
python src/scraper/telegram_scraper.py
```

### Channel Discovery
```bash
# Discover and validate channels
python src/scraper/channel_discovery.py
```

## Configuration

Edit `config/telegram_config.json` to customize:
- Channel lists
- Message limits
- Rate limiting
- Media download settings

## Data Schema

### Message Data Structure
```json
{
  "id": 12345,
  "date": "2025-07-13T18:30:00+00:00",
  "text": "Message content",
  "views": 150,
  "forwards": 5,
  "replies": 3,
  "channel": "channel_username",
  "channel_title": "Channel Display Name",
  "from_user": {
    "id": 67890,
    "username": "user123",
    "first_name": "John",
    "last_name": "Doe"
  },
  "media_info": [
    {
      "type": "photo",
      "local_path": "data/raw/telegram_images/2025-07-13/channel_name/image.jpg",
      "message_id": 12345,
      "date": "2025-07-13T18:30:00+00:00"
    }
  ]
}
```

### Metadata Schema
```json
{
  "channel": "channel_username",
  "channel_title": "Channel Display Name",
  "scrape_date": "2025-07-13T18:30:00+00:00",
  "total_messages": 1000,
  "total_media_files": 150,
  "participants_count": 5000,
  "data_file": "path/to/data.json",
  "images_directory": "path/to/images/"
}
```

## Logging

Logs are stored in `logs/telegram_scraper_YYYYMMDD.log` with the following levels:
- **INFO**: General progress updates
- **WARNING**: Non-critical issues (e.g., failed media downloads)
- **ERROR**: Channel-specific failures
- **CRITICAL**: System-level failures

## Rate Limiting

The scraper implements rate limiting to respect Telegram's API limits:
- 5-second delay between channels
- Graceful handling of rate limit errors
- Automatic retry logic for temporary failures

## Error Handling

- **Network Issues**: Automatic retry with exponential backoff
- **API Limits**: Graceful handling and logging
- **Invalid Channels**: Skip and continue with next channel
- **Media Download Failures**: Log error but continue message collection

## Next Steps

1. **Validate Channels**: Verify all unverified channels exist
2. **Data Processing**: Implement ETL pipeline for processed data
3. **Object Detection**: Train models on collected images
4. **Business Intelligence**: Create dashboards and analytics
5. **Automation**: Set up scheduled scraping

## Troubleshooting

### Common Issues

1. **ValueError: invalid literal for int()**: Check environment variables
   ```bash
   # Clear conflicting system variables
   set TELEGRAM_API_ID=
   set TELEGRAM_API_HASH=
   ```

2. **Connection Errors**: Ensure internet connection and API credentials

3. **Permission Errors**: Check if you have access to the channels

4. **Rate Limiting**: Reduce the message limit or increase delays

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is for educational and research purposes. Ensure compliance with Telegram's Terms of Service and local regulations.
