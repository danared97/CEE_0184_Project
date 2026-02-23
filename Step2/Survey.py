# import CopyEdit
import openai
from openai import OpenAI

# example
client = OpenAI(
    api_key=""
)

survey_prompt = ("Ensure that the following mail-survey questions regarding the impact of wildfires on infrastructure "
                    "such as roads, power lines, railways, and building structures are open-ended and neutral. Suggest"
                    "additional questions that may be relevant. We also want to start the survey with a clear statement of"
                    "consent and understanding that the results will be used for a study on community infrastructure recovery"
                    "post-wildfires:"
                    "Demographic questions: "
                    "Age"
                    "Gender"
                    "Ethnicity"
                    "Educational attainment"
                    "Employment status"
                    "Type of residence"
                    "Frequency with which the respondent struggles to afford essentials (options include never, rarely, sometimes, often, always)"
                    "Number of people in household"
                    "Anyone under 17 or over 65 in the household? "
                    "Whether someone in the household is dependent on other household members for their daily activities (independent living difficulties) (yes/no)"
                    "Whether someone in the household relies on devices or medications that are sensitive to electricity or temperature (electrical dependence) (yes/no)"
                    "Presence and types of pets or livestock"

                    "Power outage questions: "
                    "Did you lose power after the wildfire? yes/no"
                    "If you lost power, how did it affect you? "
                    "Did the outage raise health-related issues for you or anyone in your household?"
                    "How long was the power outage? "
                 
                    "Infrastructure questions:"
                    "Which infrastructure improvements would most support faster or more equitable recovery in your community?"
                    "How were roads and transportation affected in your area before and after the event? "
                    "Were there any changes in how you or others in your community accessed evacuation routes or emergency services? "
                    "How were railway services affected by the wildfire? Were you affected by these changes?"
                    "How were buildings and other structures in your area affected by wildfires? What was the timeline around repairing or rebuilding structures? "
                    "Did this structural damage influence your daily living conditions?"
                    
                    "Wildfire questions: "
                    "Was there any issue related to the fire that affected you  more heavily than the power outage? "
                    "How do fires affect you now compared to 10 years ago?")


response = client.responses.create(
    model="gpt-5-nano",
    input=survey_prompt,
    store=True,
)
print(response.output_text)


