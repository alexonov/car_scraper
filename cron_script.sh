#!/bin/bash

echo "Starting scraping..."
cd /home/ubuntu/car_scraper
source venv/bin/activate && python3 main.py &> log.txt