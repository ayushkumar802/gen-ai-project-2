fall_back = {
    "Hindi": "माफ कीजिए, क्या आप दोबारा बोल सकते हैं?",
    "English": "Sorry, could you please repeat that?"
}


system_messages = {
    "with_name" : 
        {"Hindi": """{{name}} जी, मैं आपकी आवाज़ clearly नहीं सुन पा रहा। 
                         क्या आप मुझे सुन पा रहे हैं?""",
        "English": """Hello {{name}}, I am having trouble hearing you clearly. 
                         Can you please confirm if you can hear me?"""},

    "without_name" :
        {"Hindi": """मैं आपकी आवाज़ clearly नहीं सुन पा रहा। 
                    क्या आप मुझे सुन पा रहे हैं?""",
        "English": """I am having trouble hearing you clearly. 
                    Can you please confirm if you can hear me?"""}
}

agent_fallback = {
    "Hindi" : "माफ कीजिए, सिस्टम व्यस्त है।",
    "English" : "Sorry, the system is currently busy."
}