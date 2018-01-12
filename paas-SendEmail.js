const nodemailer = require("nodemailer");
const fs = require("fs");

const transporter = nodemailer.createTransport({
  host: "relay.mail.ca.gov",
  port: 25,
  tls: {
    rejectUnauthorized: false
  }
});

let template = fs.readFileSync("reminderTemplate.html").toString();

let mailOptions = {
  from: "paas-do-not-reply@state.ca.gov",
  to: "",
  subject: "Reminder: Complete PAAS Approvals",
  html: template,
  attachments: [{
    filename: "odi-logo.png",
    path: "odi-logo.png",
    cid: "odi-logo"
  }]
};

module.exports = function sendEmail(fullName, email) {
  mailOptions.html = template.replace("%managerFullName%",fullName);
  mailOptions.to = email;
  // transporter.sendMail(mailOptions);
};
