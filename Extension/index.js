const { OpenAIClient, AzureKeyCredential } = require("@azure/openai");

module.exports = async function (context, req) {
  const { selectedText, userProfile } = req.body;

  const client = new OpenAIClient(
    process.env["AZURE_OPENAI_ENDPOINT"],
    new AzureKeyCredential(process.env["AZURE_OPENAI_KEY"])
  );

  const prompt = `You are a professional resume generator.\nGenerate a tailored resume based on the job description: \"${selectedText}\"\n\nUser Profile:\nName: ${userProfile.name}\nExperience: ${userProfile.experience}\nEducation: ${userProfile.education}\nSkills: ${userProfile.skills}`;

  const response = await client.getCompletions("<DEPLOYMENT_NAME>", prompt, { maxTokens: 1000 });
  const resume = response.choices[0].text;

  context.res = {
    body: { resume }
  };
};
