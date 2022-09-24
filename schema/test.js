const mongoose = require('mongoose');
const yaml = require('js-yaml');
const fs = require('fs');

// cd presist2mongo; node schema/test.js
const configPath = 'config.yml';
const config = yaml.load(fs.readFileSync(configPath, 'utf8'));

async function main() {
    const testSchema = new mongoose.Schema({}, {
        timestamps: true,
        versionKey: false
    });
    await mongoose.connect(config.base.mongo_url);
    const test = mongoose.model('instruments', testSchema);
    await test({ name: 'instruments' }).save();
    await mongoose.disconnect();
}
main()

