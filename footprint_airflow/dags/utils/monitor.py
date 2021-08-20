import requests

def send_to_slack(text):
    url = 'https://hooks.slack.com/ser'
    requests.post(url=url, json={'text': text})

def send_to_slack_and_remind_all(text):
    send_to_slack(text + '\n <!channel>')