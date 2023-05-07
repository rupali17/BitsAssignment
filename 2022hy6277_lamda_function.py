# Import Libraries

import boto3
from botocore.exceptions import ClientError
import urllib.parse
import os,io,re
from PIL import Image
from io import BytesIO

# Global Constants
AWS_REGION = "us-east-2"
CHARSET = "UTF-8"
s3_client = boto3.client("s3")
RECEPIENT_EMAIL = "2022ht66527@wilp.bits-pilani.ac.in"
SENDER_EMAIL = "candidrupali@gmail.com"

def send_email(subject, body, recipient, sender):
    """Function to send email on updates.
    """
    
    # 1. Get email client to compose and send email
    client = boto3.client("ses", region_name=AWS_REGION)

    # 2. Formulate repsonse with correct variables and collect response
    response = client.send_email(
        Destination={
            "ToAddresses": [
                recipient,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": body,
                },
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": subject,
            },
        },
        Source=sender,
    )
    
    # 3. Return Response
    return response


def lambda_handler(event, context):
    # 1. Get the S3 object key from the event
    key = event["Records"][0]["s3"]["object"]["key"]
    
    # 2. Get the S3 bucket name from the event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    
    # 3. Get the S3 bucket size from the event
    size = event["Records"][0]["s3"]["object"]["size"]
    
    # 4. Print the S3 URI
    S3_URI = f"s3://{bucket}/{key}"
    print("S3 URI : ",S3_URI)
    
    # 5. Get the object metadata from S3
    response = s3_client.head_object(Bucket=bucket, Key=key)
    
    object_size = response["ContentLength"]
    object_type = response["ContentType"]

    # 6. Check if the object is an image
    if object_type.startswith("image/"):
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        with Image.open(obj["Body"]) as img:
            # 6.1 Generate a thumbnail of the original image
            img.thumbnail((75, 75))
            orig_size = obj["ContentLength"]
            
            thumb_size = img.size
            print("Original File: {} ({} bytes)".format(key, orig_size))
            
            thumb_key = "thumbnails/" + os.path.basename(key)
            S3_Thumb_URI = "s3://{}/{}".format(bucket, thumb_key)
            thumb_F_size = thumb_size[0] * thumb_size[1] * 3
            
            # 6.2 Save Image to Inmemory Cache Bytes
            
            in_mem_file = io.BytesIO()
            img.save(in_mem_file, format=img.format)
            in_mem_file.seek(0)
            
            ## Upload image to s3
            s3_client.upload_fileobj(
                in_mem_file, # This is what i am trying to upload
                bucket,
                thumb_key,
                ExtraArgs={
                    'ACL': 'public-read'
                }
            )
            print(f"Uploaded File to {thumb_key}.")
            
        # 6.3 email subject and text manipulation
        subject = "[ALERT] New Image was Uploaded on S3"
        body = (
            "Hello Rupali!\r\n"
            "This automated email was sent from a Lambda function with the details of images and its Thumbnail.\r\n"
            "Below are the details:\r\n"
            "S3_URI: " + S3_URI + "  \n"
            "BUCKET NAME: " + bucket + " \n"
            "ORIG_F_NAME: " + key + "  \n"
            "SIZE OF IMAGE: %d" % size + "\n"
            "THUMBNAIL_FILE_NAME: " + thumb_key + "  \n"
            "SIZE_OF_THUMBNAIL: %s" % thumb_F_size + "\n"
            "S3_URI_THUMBNAILS: " + S3_Thumb_URI + "  \n"
            "Thank you for choosing this service!"
        )

        # 6.4 Call email fuction
        response = send_email(subject, body, RECEPIENT_EMAIL, SENDER_EMAIL)
        print(response)
    else:
        subject = "[ALERT] New Object was Uploaded on S3"
        body = (
            "Hello Rupali!\r\n"
            "This automated email was sent from a Lambda function with the details of object.\r\n"
            "Below the datails:\r\n"
            f"S3_URI : {S3_URI}" + "  \n"
            f"BUCKET NAME : {bucket}" + " \n"
            f"OBJECT_NAME : {key}" + "  \n"
            f"SIZE_OF_OBJECT  : {size}" + "  \n"
            "Thank you for choosing this service!"
        )
        # Call email fuction
        response = send_email(subject, body, RECEPIENT_EMAIL, SENDER_EMAIL)
        print(response)