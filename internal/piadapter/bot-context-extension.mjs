import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
export default async function (pi) {
  pi.on('before_agent_start', (event) => {
    const file = process.env.TRESORBOT_PI_INSTRUCTIONS;
    if (!file) return;
    const value = readFileSync(file, 'utf8');
    if (value.trim()) return { systemPrompt: [event.systemPrompt, value].filter(Boolean).join('\n\n') };
  });
  const url = process.env.TRESORBOT_PI_TOOL_URL;
  const token = process.env.TRESORBOT_PI_TOOL_TOKEN;
  if (!url || !token) return;
  async function call(path, body, signal) {
    const response = await fetch(url + path, {method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+token},body:JSON.stringify(body),signal});
    const result = await response.json();
    if (!response.ok) throw new Error(result.error?.code || 'Bot tool unavailable');
    return result;
  }
  const definitions = await call('/definitions',{});
  for (const tool of definitions.tools || []) {
    pi.registerTool({name:tool.name,label:tool.name,description:tool.description,parameters:tool.parameters,
      async execute(id,args,signal) {
        const operation_id=createHash('sha256').update(process.env.TRESORBOT_PI_ASSIGNMENT+'\0'+id).digest('hex');
        const result=await call('/execute',{operation_id,tool:tool.name,arguments:args},signal);
        return {content:[{type:'text',text:JSON.stringify(result)}],details:result};
      }});
  }
}
