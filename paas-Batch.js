const fetch = require('node-fetch');
const Promise = fetch.Promise = require('bluebird');
const mySQLX = require('@mysql/xdevapi');
const _ = require('lodash');
const uuid = require('uuid/v4');
const sendEmail = require('./paas-SendEmail.js');

let db, query, dBUsers = [], keyedDBUsers = [], addStaff = []
const date = new Date();
const iSODate = date.toISOString();
const authYear = (() => {
  date.setDate(date.getDate() - 30);
  return date.getFullYear();
})();

const getUUID = () => ( uuid().replace(/-/g,"").toUpperCase() )
const yearlyReset = async () => {
  await db.modify("$.status IN ('active','noManager')")
  .set('$.status', 'inactive').set('$.lastUpdated', iSODate).execute();
}
const createUser = (user) => {
  user.created = user.lastUpdated = iSODate;
  if (!user.managerSID) { user.status = "noManager"; }
	addStaff.push(user);
};
const deactivateUser = async (user) => {
  query = `($.status == "active") && ($.sid == ${JSON.stringify(user.sid)})`;
	await db.modify(query)
    .set('$.status', 'inactive').set('$.lastUpdated', iSODate).execute();
};
const updateUserFullName = async (user) => {
  query = `($.status == "active") && ($.sid == ${JSON.stringify(user.sid)})`;
  await db.modify(query)
    .set('$.fullName', user.fullName).set('$.lastUpdated', iSODate).execute();
}
const getKeyDBUsers = async () => {
  dBUsers = [];
  await db.find("$.status IN ('active','noManager')")
    .execute((doc) => { if (doc != undefined) dBUsers.push(doc); });
  keyedDBUsers = _.keyBy(dBUsers, 'sid'); 
}

(async () => {
	
	// Connect to MySQL
  const session = await mySQLX.getSession({ host: 'localhost', port: 33060, dbUser: 'root', dbPassword: '5@nj0$3@', ssl: false });
	db = session.getSchema('paas').getCollection('authorizations');

  // Yearly reset of all users on January 31st
  if (date.getMonth() === 0 && date.getDate() === 31) { yearlyReset(); };

  // Get users from the API
  const aPIUsers = await fetch('https://testedapi.technology.ca.gov/employees/bars')
    .then(response => response.json().map(x => ({
      fullName: x.fullName, sid: x.sid, email: x.email, managerFullName: x.manager, 
      managerSID: x.managerSid, status: 'active', app1: null, app2: null, app3: null, 
      app4: null, created: null, lastApproved: null, lastUpdated: null, authYear: authYear, 
      _id: getUUID()
    })))

  const keyedAPIUsers = _.keyBy(aPIUsers, 'sid'); // Key API users by SID
  await getKeyDBUsers(); // Get DB users and key by SID

  // Compare API and DB users and create/update DB users as necessary
  aPIUsers.forEach((aPIUser) => {
    if (!keyedDBUsers[aPIUser.sid]) { // User in API but not DB - Create new user
      createUser(aPIUser); 
    }
    else { // User in API and DB
      if (aPIUser.managerSID != keyedDBUsers[aPIUser.sid].managerSID) { // User has updated manager in API - Set inactive and create new user
        deactivateUser(aPIUser);
        createUser(aPIUser);
      }
      if (aPIUser.fullName != keyedDBUsers[aPIUser.sid].fullName) { // User has updated full name in API - Update full name
        updateUserFullName(aPIUser);
      }
    }
  });
  dBUsers.forEach((dBUser) => { if (!keyedAPIUsers[dBUser.sid]) { deactivateUser(dBUser); } }); // User not in API - Set inactive

  await db.add(addStaff).execute(); // Create new users in DB
	
  // Send reminder emails to managers
  if(date.getDay() == 1) { // Send only on Mondays
    await getKeyDBUsers(); // Get DB users and key by SID
    const needAuth = dBUsers.filter(dBUser => dBUser.status === "active" && dBUser.lastApproved === null);
    const byManager = _.groupBy(needAuth, 'managerSID');
    Object.keys(byManager).forEach((sid) => {
      if (keyedDBUsers[sid].fullName && keyedDBUsers[sid].email) {
        sendEmail(keyedDBUsers[sid].fullName, keyedDBUsers[sid].email);
      }
    });
  };

	session.close(); // Close MySQL connection

})();