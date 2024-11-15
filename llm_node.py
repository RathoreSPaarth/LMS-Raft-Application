import grpc
from concurrent import futures
import time
import lms_pb2
import lms_pb2_grpc
import openai

# Initialize OpenAI API key
openai.api_key = 'sk-Q0bZUA0Zp8OvWygfYczUeXGrMx-sOGd5WK36nqMmy3T3BlbkFJJb6JOvr-2w_ToBdPA26fAqMteG9T8SCrxOvnfw9qEA'

class TutoringServerServicer(lms_pb2_grpc.TutoringServerServicer):
    def getLLMAnswer(self, request, context):
        query = request.query
        input_text = f"Question: {query}"
        
        try:
            # Make an API call to GPT-3.5 using the ChatCompletion endpoint
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",  # Correct model name for GPT-3.5
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},  # Set assistant behavior
                    {"role": "user", "content": input_text},  # The query from the user
                ],
                max_tokens=100,
                temperature=0.4,
                top_p=0.9,
                n=1
            )
            
            # Extract the generated response
            generated_text = response['choices'][0]['message']['content'].strip()
        except Exception as e:
            print(f"Error calling OpenAI API: {e}")
            generated_text = "Error generating answer."
        
        return lms_pb2.getLLMAnswerResponse(queryId=request.queryId, answer=generated_text)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lms_pb2_grpc.add_TutoringServerServicer_to_server(TutoringServerServicer(), server)
    server.add_insecure_port('172.17.49.232:50055')  # Port for Tutoring Server
    server.start()
    print("Tutoring Server started on port 50055.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__': 
    serve()
