import grpc
from concurrent import futures
import time
import lms_pb2
import lms_pb2_grpc
from transformers import pipeline, GPT2Tokenizer, GPT2LMHeadModel

# Initialize the lightweight LLM model (GPT-2 or similar)
# model = pipeline('text-generation', model='gpt2')
tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
model = GPT2LMHeadModel.from_pretrained("gpt2")

class TutoringServerServicer(lms_pb2_grpc.TutoringServerServicer):
    def getLLMAnswer(self, request, context):
        query = request.query
        # prompt = "You are an experienced and patient tutor specializing in Computer Science. Give clear and concise answers to the queries. "
        # prompt = f"You are an experienced and patient tutor specializing in Computer Science. Give clear and concise answers to the queries. {query}\nTutor:"
        context = """You are an experienced and patient tutor specializing in Computer Science. Your role is to assist students with their questions on topics like algorithms, data structures, programming languages, databases, and more."""
        
        input_text = f"Context: {context} Question: {query}"
        # Tokenize the input
        input_ids = tokenizer.encode(input_text, return_tensors='pt')

        # Generate a response using the LLM
        # response = model(input_text, max_length=2000, temperature = 0.7, top_p = 0.9, top_k = 50, truncation = True, do_sample = True)[0]['generated_text']
        # Generate the answer using GPT-2
        outputs = model.generate(input_ids, max_length=62, num_return_sequences=1, temperature=0.4, top_p=0.9, do_sample = True)
        # Decode the output
        generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        return lms_pb2.getLLMAnswerResponse(queryId=request.queryId, answer=generated_text)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lms_pb2_grpc.add_TutoringServerServicer_to_server(TutoringServerServicer(), server)
    server.add_insecure_port('[::]:50052')  # Port for Tutoring Server
    server.start()
    print("Tutoring Server started on port 50052.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()


# Source: ChatGPT