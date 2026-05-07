const users = [
  {
    id: 1,
    name: 'Maya Perera',
    role: 'Platform Engineer',
    team: 'DevOps',
    active: true
  },
  {
    id: 2,
    name: 'Ravi Fernando',
    role: 'Backend Engineer',
    team: 'Monitoring',
    active: true
  },
  {
    id: 3,
    name: 'Nadia Silva',
    role: 'QA Engineer',
    team: 'Release',
    active: false
  },
  {
    id: 4,
    name: 'Ishan De Alwis',
    role: 'Data Analyst',
    team: 'Sustainability',
    active: true
  }
];

function getAllUsers() {
  return users.map((user) => ({ ...user }));
}

function getUserById(id) {
  const user = users.find((entry) => entry.id === id);
  return user ? { ...user } : null;
}

module.exports = {
  getAllUsers,
  getUserById
};
