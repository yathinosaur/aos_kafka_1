import json
from time import sleep

from aos.checker import instanceof
from aos.infer import infer_aos

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'parsed_recipes'
    # Notify if a recipe has more than 200 calories
    calories_threshold = 200

    aos_check = '((title & str) | (submitter & str) | (description & str) | (calories & str) | (ingredients & (((step & str)) *)))'

    while(True):
        consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                                bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        for msg in consumer:
            record = json.loads(msg.value)
            #print(infer_aos(record))
            if(not instanceof(record, aos_check)):
                print("INVALID MESSAGE: " + record)
            calories = int(record['calories'])
            title = record['title']

            if calories > calories_threshold:
                print('Alert: {} calories count is {}'.format(title, calories))
            sleep(3)

        if consumer is not None:
            consumer.close()
        sleep(2)