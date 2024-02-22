
import threading
import time
import cv2
import boto3


class RTSPCapture:
    def __init__(self, rtsp_url, output_file, interval):
        self.rtsp_url = rtsp_url
        self.output_file = output_file
        self.interval = interval
        self.cap = cv2.VideoCapture(self.rtsp_url)
        if not self.cap.isOpened():
            print("Error: Unable to open RTSP stream")
            return

    def capture_frames(self):
        print(self.rtsp_url)
        start_time = time.time()
        ret, frame = self.cap.read()
        if not ret:
            return
        out = cv2.VideoWriter(f'{self.output_file}_{time.time()}.avi', cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'), 10,
                              (frame.shape[1], frame.shape[0]))

        while True:
            ret, frame = self.cap.read()
            if not ret:
                break
            out.write(frame)
            cv2.imshow("foo", frame)
            cv2.waitKey(1)
            end_time = time.time()
            if end_time - start_time > self.interval:
                break
        out.release()

    def start(self):
        capture_thread = threading.Thread(target=self.capture_frames)
        capture_thread.start()


if __name__ == "__main__":
    rtsp_url = "rtsp://jtuser:PASS4jtuser@192.168.1.95:8555/Stream4"
    output_file_prefix = "rtsp_clips"
    rtsp_capture = RTSPCapture(rtsp_url, output_file_prefix, 60)
    max_clips = 1
    clips_captured = 0

    while clips_captured < max_clips:
        rtsp_capture.capture_frames()
        clips_captured += 1
        print(f"Captured {clips_captured} streams")

    # Initialize S3 client with the correct region

s3 = boto3.client('s3', region_name='us-east-1')

# Open the file to upload
with open(r'rtsp_clips_1708535889.9887254.avi', 'rb') as data:
    # Upload the file to the specified S3 bucket and key
    s3.upload_fileobj(data, 'insites-live-view-dev', 'rtsp_clips_1708535889.9887254.avi')



timestream_client = boto3.client('timestream-write', region_name='us-east-1')

print("Maximum number of clips captured. Stopping the capture process")
