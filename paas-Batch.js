const fetch = require('node-fetch');
const Promise = fetch.Promise = require('bluebird');
const MongoClient = require('mongodb').MongoClient
const _ = require('lodash');
const sendEmail = require('./paas-SendEmail.js');

const url = 'mongodb://localhost:27017';
const colName = 'users';

let updates = [];

const getDate = () => new Date().toISOString();
const createUser = (user) => {
  user.created = user.lastUpdated = getDate();
  updates.push({ insertOne: { document: user } });
};
const deativateUser = (user) => {
  updates.push({ updateOne: { filter: { sid: user.sid, status: 'active' }, update: { $set: { status: 'inactive', lastUpdated: getDate() } } } });
};

(async () => {

  // Connect to Mongo
  const client = await MongoClient.connect(url);
  const Users = client.db("paas").collection(colName);

  // Get users from the API
  const aPIUsers = await fetch('https://testedapi.technology.ca.gov/employees/bars')
    .then(response => response.json().map(x => ({
      fullName: x.fullName, sid: x.sid, email: x.email, managerFullName: x.manager,
      managerSID: x.managerSid, status: 'active', app1: null, app2: null, app3: null,
      app4: null, created: null, lastApproved: null, lastUpdated: null
    })))

  const keyedAPIUsers = _.keyBy(aPIUsers, 'sid'); // Key API users by SID

  let dBUsers = await Users.find({ status: 'active' }).toArray(); // Retrieve all active DB users
  let keyedDBUsers = _.keyBy(dBUsers, 'sid'); // Key all active DB users by SID

  // Compare API and DB users and stage all creates/updates to be performed in DB
  aPIUsers.forEach((aPIUser) => { // User in API but not DB
    if (!keyedDBUsers[aPIUser.sid]) { createUser(aPIUser); } // Create new user
    else { // User in API and DB
      if (aPIUser.managerSID != keyedDBUsers[aPIUser.sid].managerSID) { // User has updated manager in API - Set inactive and create new user
        deativateUser(aPIUser);
        createUser(aPIUser);
      }
      if (aPIUser.fullName != keyedDBUsers[aPIUser.sid].fullName) { // User has updated full name in API - Update full name
        updates.push({ updateOne: { filter: { sid: aPIUser.sid, status: 'active' }, update: { $set: { fullName: aPIUser.fullName, lastUpdated: getDate() } } } });
      }
    }
  });
  dBUsers.forEach((dBUser) => { if (!keyedAPIUsers[dBUser.sid]) { deativateUser(dBUser); } }); // User not in API - Set inactive

  updates.length && await Users.bulkWrite(updates); // Perform all creates/updates in DB

  // Send reminder emails to managers
  if(today.getDay() == 1) { // Send only on Mondays
    dBUsers = await Users.find({ status: 'active' }).toArray(); // Retrieve all active DB users
    keyedDBUsers = _.keyBy(dBUsers, 'sid'); // Key all active DB users by SID
    let byManager = await Users.aggregate([
      { $match: { $and: [ { status: "active" }, { lastApproved: null }, { managerSID: { $ne: '' } } ] } },
      { $group: { _id: "$managerSID" } }
    ]).toArray();
    byManager.forEach((manager) => {
      if (keyedDBUsers[manager._id].fullName && keyedDBUsers[manager._id].email ) {
        sendEmail(keyedDBUsers[manager._id].fullName, keyedDBUsers[manager._id].email);
      }
    });
  };  

  client.close(); // Close Mongo connection

})();