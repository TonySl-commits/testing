from langchain.prompts import ChatPromptTemplate
from langchain_groq.chat_models import ChatGroq
import json

class GroqModel:
    def __init__(self, system_prompt):
        # self.chat = ChatGroq(temperature=0, model_name="mixtral-8x7b-32768")
        self.chat = ChatGroq(temperature=0, model_name="llama3-70b-8192", api_key= "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")
        self.system = system_prompt
        self.human = "{text}"
        self.prompt = ChatPromptTemplate.from_messages([("system", self.system), ("human", self.human)])

        self.chain = self.prompt | self.chat

    def generate_response(self, text):
            response = self.chain.invoke({"text": text})
            res = response.json()
            res_formated = json.loads(res)
            return res_formated['content']

def main():
    model = GroqModel("you are a helpful assistant")
    text = "What is the meaning of life?"
    print(model.generate_response(text))