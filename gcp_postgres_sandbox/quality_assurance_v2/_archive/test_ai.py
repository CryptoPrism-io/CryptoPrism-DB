#!/usr/bin/env python3
"""
Test AI integration for QA system.
"""

import os
import sys
from pathlib import Path

# Add paths for imports
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(current_dir))

from dotenv import load_dotenv

# Load environment - Override existing system variables
load_dotenv(current_dir.parent.parent / ".env", override=True)

def test_gemini_ai():
    """Test Google Gemini AI integration."""
    print("Testing Google Gemini AI...")
    
    try:
        import google.generativeai as genai
        
        api_key = os.getenv("GEMINI_API_KEY")
        print(f"API Key found: {api_key[:20]}..." if api_key else "No API key found")
        
        if not api_key:
            print("No Gemini API key found in environment")
            return False
        
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        # Simple test prompt
        test_prompt = "Summarize this in one sentence: The database has 100 checks, 2 critical issues found."
        
        response = model.generate_content(test_prompt)
        
        if response and response.text:
            print(f"AI Response: {response.text}")
            return True
        else:
            print("No response from Gemini AI")
            return False
            
    except ImportError:
        print("google-generativeai library not installed")
        return False
    except Exception as e:
        print(f"Gemini AI test failed: {e}")
        return False

def test_openai():
    """Test OpenAI integration."""
    print("Testing OpenAI integration...")
    
    try:
        import openai
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("No OpenAI API key found in environment")
            return False
        
        client = openai.OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a database QA analyst. Provide concise summaries."},
                {"role": "user", "content": "Summarize this in one sentence: The database has 100 checks, 2 critical issues found."}
            ],
            max_tokens=100
        )
        
        if response and response.choices:
            print(f"OpenAI Response: {response.choices[0].message.content}")
            return True
        else:
            print("No response from OpenAI")
            return False
            
    except ImportError:
        print("openai library not installed")
        return False
    except Exception as e:
        print(f"OpenAI test failed: {e}")
        return False

if __name__ == "__main__":
    print("=== AI Integration Testing ===")
    
    gemini_works = test_gemini_ai()
    print()
    openai_works = test_openai()
    
    print("\n=== Results ===")
    print(f"Gemini AI: {'Working' if gemini_works else 'Not Working'}")
    print(f"OpenAI: {'Working' if openai_works else 'Not Working'}")
    
    if not gemini_works and not openai_works:
        print("\nNeither AI service is working. You can:")
        print("1. Fix the Gemini API key")
        print("2. Add an OpenAI API key to .env as OPENAI_API_KEY")
        print("3. Continue using QA system without AI summaries")