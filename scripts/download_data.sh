#!/bin/bash
# ============================================
# NYC Taxi Pipeline - Data Download Script
# ============================================
# Downloads NYC TLC Yellow Taxi trip data
#
# Usage:
#   ./download_data.sh 2023-01
#   ./download_data.sh 2023-01 2023-02 2023-03
#   make download-data MONTHS="2023-01 2023-02"
# ============================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base URLs
TRIP_DATA_BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Default data directory
DATA_DIR="${DATA_PATH:-./data}"
RAW_DIR="${DATA_DIR}/raw"

# ============================================
# Functions
# ============================================

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    echo "Usage: $0 <month1> [month2] [month3] ..."
    echo ""
    echo "Examples:"
    echo "  $0 2023-01                    # Download January 2023"
    echo "  $0 2023-01 2023-02 2023-03    # Download Q1 2023"
    echo ""
    echo "Month format: YYYY-MM"
    exit 1
}

download_file() {
    local url=$1
    local output=$2
    local description=$3

    if [ -f "$output" ]; then
        print_warning "$description already exists, skipping: $output"
        return 0
    fi

    print_info "Downloading $description..."
    print_info "  URL: $url"
    print_info "  Output: $output"

    if curl -L --progress-bar -o "$output" "$url"; then
        print_success "Downloaded: $output"
        return 0
    else
        print_error "Failed to download: $url"
        rm -f "$output"  # Remove partial file
        return 1
    fi
}

download_trip_data() {
    local month=$1
    local filename="yellow_tripdata_${month}.parquet"
    local url="${TRIP_DATA_BASE_URL}/${filename}"
    local output="${RAW_DIR}/${filename}"

    download_file "$url" "$output" "Yellow Taxi Trip Data ($month)"
}

download_zone_lookup() {
    local filename="taxi_zone_lookup.csv"
    local output="${RAW_DIR}/${filename}"

    download_file "$ZONE_LOOKUP_URL" "$output" "Taxi Zone Lookup"
}

# ============================================
# Main
# ============================================

# Check arguments
if [ $# -eq 0 ]; then
    show_usage
fi

# Validate month format
for month in "$@"; do
    if ! [[ $month =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
        print_error "Invalid month format: $month"
        print_error "Expected format: YYYY-MM (e.g., 2023-01)"
        exit 1
    fi
done

# Create directories
print_info "Creating directories..."
mkdir -p "$RAW_DIR"

# Download zone lookup (only once)
download_zone_lookup

# Download trip data for each month
for month in "$@"; do
    download_trip_data "$month"
done

# Summary
echo ""
print_success "Download complete!"
echo ""
print_info "Files downloaded to: $RAW_DIR"
ls -lh "$RAW_DIR"
echo ""
print_info "Next steps:"
echo "  1. make start              # Start Docker services"
echo "  2. make run-bronze MONTHS=\"$*\""
