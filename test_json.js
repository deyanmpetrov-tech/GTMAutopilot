const fs = require('fs');
const data = JSON.parse(fs.readFileSync('output_test.json'));
let error = false;
data.containerVersion.tag.forEach(t => {
   if (!t.tagFiringOption) { console.log('Missing tagFiringOption in tag:', t.name); error=true;}
});
if(!error) console.log("All tags have tagFiringOption");
