const fs = require('fs');
const path = require('path');

const rootDir = path.resolve(__dirname, '..');
const srcDir = path.join(rootDir, 'src');
const distDir = path.join(rootDir, 'dist');

build();

function build() {
  cleanDist();
  copyDirectory(srcDir, distDir);
  validateBuildOutput();
  simulateBuildWork();
  console.log('Build completed successfully.');
}

function cleanDist() {
  fs.rmSync(distDir, { recursive: true, force: true });
  fs.mkdirSync(distDir, { recursive: true });
}

function copyDirectory(sourceDir, targetDir) {
  fs.mkdirSync(targetDir, { recursive: true });

  const entries = fs.readdirSync(sourceDir, { withFileTypes: true });

  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    const targetPath = path.join(targetDir, entry.name);

    if (entry.isDirectory()) {
      copyDirectory(sourcePath, targetPath);
    } else {
      fs.copyFileSync(sourcePath, targetPath);
    }
  }
}

function validateBuildOutput() {
  const requiredFiles = [
    'app.js',
    path.join('routes', 'health.routes.js'),
    path.join('routes', 'users.routes.js'),
    path.join('routes', 'analytics.routes.js'),
    path.join('services', 'users.service.js'),
    path.join('services', 'analytics.service.js')
  ];

  for (const relativeFile of requiredFiles) {
    const filePath = path.join(distDir, relativeFile);

    if (!fs.existsSync(filePath)) {
      throw new Error(`Missing build artifact: ${relativeFile}`);
    }

    const contents = fs.readFileSync(filePath, 'utf8');
    if (!contents.includes('module.exports') && !contents.includes('express')) {
      throw new Error(`Validation failed for: ${relativeFile}`);
    }
  }
}

function simulateBuildWork() {
  let checksum = 0;

  for (let index = 0; index < 350000; index += 1) {
    checksum += Math.sqrt((index % 113) + 1) * Math.sin(index / 19);
  }

  if (!Number.isFinite(checksum)) {
    throw new Error('Build simulation produced an invalid checksum');
  }
}
