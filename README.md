1. Перед початком роботи запустіть ./config.sh
2. Скріпт запитає ваш YouTube API Key, згенеруйте його завчасно у своєму кабінеті
3. Активуйте віртуальне середовище: ```source .venv/bin/activate```, якщо це ще не було зроблено
4. Перейдіть у папку зі скриптами ```cd src```
5. За потреби змініть/доповніть файл *data/youtube_channels_list.json* з переліком бажаних каналів
6. Запустіть ```python get_channel_ids.py``` щоб дістати унікальні id вказаних вами каналів. Він збереже їх у *data/channel_ids.json*
7. Запустіть ```python get_video_urls.py``` щоб отримати список url з відео цих каналів
8. Запустіть ```python get_comments.py``` щоб викачати коментарі. В коді скріпта вкажіть коректне значення кількості потоків для розпаралелення роботи (зазвичай до 32)
9. Запустіть ```python extract_texts.py``` щоб злити всі коменти в один файл, а також очистити їх від зайвої метаінформації
