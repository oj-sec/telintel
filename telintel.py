import json
import asyncio
import logging
import re
import hashlib
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient 
import telethon.tl as tl
from telethon.tl.functions.channels import JoinChannelRequest
from telethon import functions, types
from telethon.tl.types import InputPeerEmpty


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# JSON selialisation helper
class DateTimeEncoder(json.JSONEncoder):
    def default(self, object):
        if isinstance(object, datetime):
            return object.isoformat()
        if isinstance(object, bytes):
            return str(object)
        return json.JSONEncoder.default(self, object)


class Telintel():

    def __init__(self, config=None, config_file="config.json", bot=False):
        # Config redundancy
        if not config:
            try:
                with open(config_file, "r") as c:
                    config = json.loads(c.read())
            except:
                logging.critical(f"{datetime.now(timezone.utc).isoformat()} - CRITICAL: No configuration information or file provided.")

        # Handle non-bot case
        if not bot:
            try:
                self.client = TelegramClient('telintel',  config['telegramApiID'], config['telegramApiHash'])
                self.client.connect()
                logging.info(f"{datetime.now(timezone.utc).isoformat()} - INFO: Telegram client created succesfully.")
            except Exception as e:
                logging.critical(f"{datetime.now(timezone.utc).isoformat()} - CRITICAL: Failed to create Telegram client with exception: {str(e)}")
        else:
            self.client = TelegramClient('telintel_bot',  config['telegramApiID'], config['telegramApiHash']).start(bot_token=config['botToken'])
            #self.client.connect()

        self.download_directory = config['downloadPath']
        self.excluded_mime_types = config['excludedMimeTypes']

    async def get_entity_handle(self, id):
        try:
            handle = await self.client.get_entity(id)
            logging.info(f"{datetime.now(timezone.utc).isoformat()} - Info: Created handle to channel {id}.")
        except:
            logging.warning(f"{datetime.now(timezone.utc).isoformat()} - Warning: Failed to create handle to chanel {id}.")
            return None
        return handle
    
    async def get_entity_dict(self, id):
        handle = await self.get_entity_handle(id)
        dict = handle.to_dict()
        return dict
    
    # no optional parameter will iter all messages
    # max_id will iter messages above the passed integer id
    # newer_than will iter messages newer than the passed datetime object
    # older_than will iter messages older than the passed datetime object
    async def iter_channel_messages(self, channel_handle, max_id=None, newer_than=None, older_than=None, download=False):
        # Ensure the client is a member of the channel
        try:
            await self.client(JoinChannelRequest(channel_handle))
        except:
            logging.critical(f"{datetime.now(timezone.utc).isoformat()} - CRITICAL: Unable to access or join channel {channel_handle.to_dict()['id']}")
            return
        
        messages = []

        # Note that this does not currently obtain 'comments' to channel posts.
        # Channel comments can be iterated with a double context loop like:
        # async for message in self.client.iter_messages(channel_handle, max_id=max_id):
        #   message = message.to_dict()
        #   async for reply in self.client.iter_messages(channel_handle, reply_to=message['id']):
        #       reply = reply.to_dict()
        # However, comments seem to be ablt to be from a different channel than the input, 
        # so this may need some post-processing considdered to avoid orphaned data. 
        logging.info(f"{datetime.now(timezone.utc).isoformat()} - INFO: Starting iteration of messages in channnel: {channel_handle.to_dict()['id']}")
    
        if max_id:
            async for message in self.client.iter_messages(channel_handle, max_id=max_id):
                if download:
                    download_record = await self.download_documents(message)
                message = message.to_dict()
                message['telintel'] = {}
                message['telintel']['enrichments'] = await self.enrich_message(message)
                if download_record:
                    message['telintel']['download_record'] = download_record
                    print(json.dumps(message, indent=4, cls=DateTimeEncoder))
                messages.append(message)
        elif newer_than:
            async for message in self.client.iter_messages(channel_handle, reverse=True, offset_date=newer_than):
                if download:
                    download_record = await self.download_documents(message)
                message = message.to_dict()
                message['telintel'] = {}
                message['telintel']['enrichments'] = await self.enrich_message(message)
                if download_record:
                    message['telintel']['download_record'] = download_record
                    print(json.dumps(message, indent=4, cls=DateTimeEncoder))
                messages.append(message)
        elif older_than:
            async for message in self.client.iter_messages(channel_handle, offset_date=older_than):
                if download:
                    download_record = await self.download_documents(message)
                message = message.to_dict()
                message['telintel'] = {}
                message['telintel']['enrichments'] = await self.enrich_message(message)
                if download_record:
                    message['telintel']['download_record'] = download_record
                    print(json.dumps(message, indent=4, cls=DateTimeEncoder))
                messages.append(message)
        else:
            async for message in self.client.iter_messages(channel_handle):
                if download:
                    download_record = await self.download_documents(message)
                message = message.to_dict()
                message['telintel'] = {}
                message['telintel']['enrichments'] = await self.enrich_message(message)
                if download_record:
                    message['telintel']['download_record'] = download_record
                    print(json.dumps(message, indent=4, cls=DateTimeEncoder))
                messages.append(message)
                
        logging.info(f"{datetime.now(timezone.utc).isoformat()} - INFO: Completed iteration of messages in channnel: {channel_handle.to_dict()['id']}. Obtained {len(messages)} messages.")

        return messages

    # Inefficient message iterator that must be used when operating as a bot.
    async def iter_messages_naive(self, dialog_handle, offset=1, download=False):

        collected_messages = []
        consecutive_empty = 0

        logging.info(f"{datetime.now(timezone.utc).isoformat()} - INFO: Starting naive iteration of messages in channnel: {dialog_handle.to_dict()['id']}")

        while True:

            messages = await self.client.get_messages(1909112828, limit=100, min_id=offset, max_id=offset + 99, ids=list(range(offset, offset + 99)))
            offset = offset + 100

            print(messages.__str__())

            empty_flag = True
            
            for message in messages:
                if message:
                    empty_flag = False
                    download_record = await self.download_documents(message)
                    message = message.to_dict()
                    message['telintel'] = {}
                    message['telintel']['download_record'] = download_record
                    collected_messages.append(message)

                    with open('temp','a') as f:
                        json.dump(message, f, cls=DateTimeEncoder)
                        f.write("\n")

            if empty_flag:
                consecutive_empty = consecutive_empty + 1
            else:
                consecutive_empty = 0
            if consecutive_empty == 4:
                break

        logging.info(f"{datetime.now(timezone.utc).isoformat()} - INFO: Finished naive iteration of messages in channnel: {dialog_handle.to_dict()['id']}. Collected {len(collected_messages)} messages.")

        return collected_messages

    async def enrich_message(self, message_dict):
        enrichments = {}
        # Parse links via "@" tags
        if "message" in message_dict.keys():
            if "@" in message_dict['message']:
                enrichments["tags"] = []
                tags = re.findall(r'\@\w*', message_dict['message'])
                for tag in tags:
                    try:
                        tag_entity = await self.client.get_entity(tag.lstrip("@"))
                        tag_id = tag_entity.id
                    except:
                        tag_id = ""
                    enrichments["tags"].append({"tag_name":tag.lstrip("@"),"id":tag_id})
        return enrichments

    async def download_documents(self, message):
        # Fail early if not dealing with a 'document' media type
        try:
            mime_type = message.media.document.mime_type
        except:
            return
        if mime_type not in self.excluded_mime_types:
            blob = await self.client.download_media(message, bytes)
            file_hash = hashlib.sha256(blob).hexdigest()
            with open(f"{self.download_directory}/{file_hash}", "wb") as f:
                f.write(blob)
            logging.debug(f"{datetime.now(timezone.utc).isoformat()} - DEBUG: Downloaded file: {file_hash}")
            download_record = { "sha256" :file_hash,"download_time": str(datetime.now(timezone.utc).isoformat()) }
            return download_record
        

# Example usage

telintel = Telintel()
loop = asyncio.get_event_loop()

with telintel.client:

    print(loop.run_until_complete(telintel.get_entity_dict(-1001857526185)))
    channel = loop.run_until_complete(telintel.get_entity_handle(-1001857526185))
    messages = loop.run_until_complete(telintel.iter_messages_naive(channel, download=True))

    with open("temp",'a') as f:
        for message in messages:
            json.dump(message, f, indent=4, cls=DateTimeEncoder)
            f.write("\n")

        
