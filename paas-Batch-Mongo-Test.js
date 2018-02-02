const fetch = require('node-fetch');
const Promise = fetch.Promise = require('bluebird');
const MongoClient = require('mongodb').MongoClient // **Mongo**
const mySQLX = require('@mysql/xdevapi');
const _ = require('lodash');
const sendEmail = require('./paas-SendEmail.js');

const url = 'mongodb://localhost:27017'; // **Mongo**
const URL = '';
const colName = 'testusers';

let newUsers = 0, deactivateUsers = 0, updateManagers = 0, updateFullNames = 0;  // **Testing**
let updates = [];
const date = new Date();
const iSODate = date.toISOString();
const authYear = (() => {
  date.setDate(date.getDate() - 30);
  return date.getFullYear();
})();
const isResetDate = () => (date.getMonth() === 0 && date.getDate() === 31)
const createUser = (user) => {
  user.created = user.lastUpdated = iSODate;
  if (!user.managerSID) { user.status = "noManager"; }
  updates.push({ insertOne: { document: user } });
  newUsers++; // **Testing**
};
const deativateUser = (user) => {
  updates.push({ updateOne: { filter: { sid: user.sid, status: 'active' }, update: { $set: { status: 'inactive', lastUpdated: iSODate } } } });
  deactivateUsers++; // **Testing**
};

(async () => {

  // Connect to Mongo
  const client = await MongoClient.connect(url);
  const Users = client.db("paas").collection(colName);

  // Connect to MySQL
  const session = await mySQLX.getSession({ host: 'localhost', port: 33060, dbUser: 'root', dbPassword: '5@nj0$3@', ssl: false });
  const database = session.getSchema('paas');
  
  if (isResetDate()) { await Users.updateMany({}, { $set: { status: "inactive" } }); } // Yearly reset of all users on January 31st

  // Get users from the API
  const aPIUsers = await fetch('https://testedapi.technology.ca.gov/employees/bars')
    .then(response => response.json().map(x => ({
      fullName: x.fullName, sid: x.sid, email: x.email, managerFullName: x.manager,
      managerSID: x.managerSid, status: 'active', app1: null, app2: null, app3: null,
      app4: null, created: null, lastApproved: null, lastUpdated: null, authYear: authYear
    })))

  // **Testing**
  // await Users.deleteMany({}); // Delete all users in DB
  // aPIUsers[766].managerSID = "Changed Manager SID"; // Change API data: A user has a new manager
  // aPIUsers[766].managerFullName = "Changed Manager Full Name"; // Change API data: A user has a new manager
  // aPIUsers[766].fullName = "Changed Name"; // Change API data: A user has a new full name
  // aPIUsers.push({ // Change API data: A new user
  //   fullName: 'New Name', sid: 'New SID', email: 'john.jones@state.ca.gov', 
  //   managerFullName: 'New Manager Name', managerSID: 'New Manager SID', status: 'active', 
  //   app1: null, app2: null, app3: null, app4: null, created: null, lastApproved: null, 
  //   lastUpdated: null
  // });
  // Users.updateOne({ fullName: "Igor Pekelis", status: "active"},{ $set: { status: "inactive"} }); // Set a record inactive

  const keyedAPIUsers = _.keyBy(aPIUsers, 'sid'); // Key API users by SID

  let dBUsers = await Users.find({ $or: [{ status: "active" }, { status: "noManager" }] }).toArray(); // Retrieve all active DB users
  let keyedDBUsers = _.keyBy(dBUsers, 'sid'); // Key all active DB users by SID

  // Compare API and DB users and stage all creates/updates to be performed in DB
  aPIUsers.forEach((aPIUser) => { // User in API but not DB
    if (!keyedDBUsers[aPIUser.sid]) { createUser(aPIUser); } // Create new user
    else { // User in API and DB
      if (aPIUser.managerSID != keyedDBUsers[aPIUser.sid].managerSID) { // User has updated manager in API - Set inactive and create new user
        deativateUser(aPIUser);
        createUser(aPIUser);
        updateManagers++; // **Testing**
      }
      if (aPIUser.fullName != keyedDBUsers[aPIUser.sid].fullName) { // User has updated full name in API - Update full name
        updates.push({ updateOne: { filter: { sid: aPIUser.sid, status: 'active' }, update: { $set: { fullName: aPIUser.fullName, lastUpdated: iSODate } } } });
        updateFullNames++; // **Testing**
      }
    }
  });
  dBUsers.forEach((dBUser) => { if (!keyedAPIUsers[dBUser.sid]) { deativateUser(dBUser); } }); // User not in API - Set inactive

  // updates.length && await Users.bulkWrite(updates); // Perform all creates/updates in DB

  // Send reminder emails to managers
  dBUsers = await Users.find({ $or: [{ status: "active" }, { status: "noManager" }] }).toArray(); // Retrieve all active/noManager DB users
  keyedDBUsers = _.keyBy(dBUsers, 'sid'); // Key all active DB users by SID
  let byManager = await Users.aggregate([
    { $match: { $and: [{ status: "active" }, { lastApproved: null }, { managerSID: { $ne: '' } }] } },
    { $group: { _id: "$managerSID" } }
  ]).toArray();
  let emailCount = 0; // *Testing**
  byManager.forEach((manager) => {
    if (keyedDBUsers[manager._id].fullName && keyedDBUsers[manager._id].email) {
      sendEmail(keyedDBUsers[manager._id].fullName, keyedDBUsers[manager._id].email);
      emailCount++ // *Testing**
    }
  });

  // **Testing**
  const dBUsersCount = await Users.find({}).toArray();
  const dBUsersActiveCount = await Users.find({ status: 'active' }).toArray();
  console.log("Users in DB: " + dBUsersCount.length);
  console.log("Active Users in DB: " + dBUsersActiveCount.length);
  console.log("Create new user(s): " + newUsers);
  console.log("Deactivate user(s): " + deactivateUsers);
  console.log("Update manager(s): " + updateManagers);
  console.log("Update full name(s): " + updateFullNames);
  console.log("Reminder email(s): " + emailCount);
  const users1 = await Users.find({ sid: aPIUsers[766].sid }).project({ _id: 0, sid: 1, fullName: 1, managerSID: 1, managerFullName: 1, status: 1 }).toArray();
  console.log(users1);
  const users2 = await Users.find({ sid: 'New SID' }).project({ _id: 0, sid: 1, fullName: 1, managerSID: 1, managerFullName: 1, status: 1 }).toArray();
  console.log(users2);

  client.close(); // Close Mongo connection

})();