#!/bin/bash

API_FILE="youtube_api_key.txt"
CHANNELS_LIST_FILE="./data/youtube_channels_list.json"

read -p "Введіть ваш API ключ: " api_key
echo "$api_key" > $API_FILE

mkdir data

cat <<EOF > $CHANNELS_LIST_FILE
{
  "channels": [
    "Latexfauna",
    "Курган і Агрегат"
  ]
}
EOF

echo "Тепер можете оновити $CHANNELS_LIST_FILE списком бажаних каналів."
