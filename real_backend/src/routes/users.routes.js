const express = require('express');

const {
  getAllUsers,
  getUserById
} = require('../services/users.service');

const router = express.Router();

router.get('/', (req, res) => {
  const users = getAllUsers();
  res.json({
    count: users.length,
    users
  });
});

router.get('/:id', (req, res) => {
  const userId = Number.parseInt(req.params.id, 10);

  if (Number.isNaN(userId)) {
    return res.status(400).json({
      error: 'Bad Request',
      message: 'User id must be a number'
    });
  }

  const user = getUserById(userId);

  if (!user) {
    return res.status(404).json({
      error: 'Not Found',
      message: `User ${userId} was not found`
    });
  }

  return res.json(user);
});

module.exports = router;
