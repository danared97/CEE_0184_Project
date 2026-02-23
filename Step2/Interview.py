#import CopyEdit
import openai
from openai import OpenAI

# example
client = OpenAI(
  api_key=""
)

interview_prompt = ("Ensure that the following interview questions regarding the impact of wildfires on infrastructure "
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
            
                    "General questions: "
                    "Did you receive any aid from the government or from your power provider during or after this event? "
                    "If you did receive aid, what was most helpful for you during/after this event?"
                    "What kind of aid would have helped you the most after this event? "
                    "Are there any ways that this event still affects you? How?"
                    "What was the worst outcome from this event for you? "
                    "Was your job affected by this event?"
                    "How do you get to work? Was your route to work affected by this event?")

response = client.responses.create(
  model="gpt-5-nano",
  input=interview_prompt,
  store=True,
)
print(response.output_text)


