'use strict';

module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
    jshint: {
      options: {
        jshintrc: '.jshintrc'
      },
      gruntfile: {
        src: 'Gruntfile.js'
      },
      main: {
        src: ['index.js']
      },
      lib: {
        src: ['lib/**/*.js']
      },
      test: {
        src: ['test/**/*.js']
      },
      examples: {
        src: ['examples/**/*.js']
      },
    },
    watch: {
      gruntfile: {
        files: '<%= jshint.gruntfile.src %>',
        tasks: ['jshint:gruntfile']
      },
      lib: {
        files: '<%= jshint.lib.src %>',
        tasks: ['jshint:lib', 'simplemocha']
      },
      test: {
        files: '<%= jshint.test.src %>',
        tasks: ['jshint:test', 'simplemocha']
      },
    },
    simplemocha: {
      all: {
        src: ['test/**/*.js'],
        options: {
          globals: ['chai'],
          timeout: 2000,
          ignoreLeaks: false,
          reporter: 'dot'
        }
      }
    },
    clean: ['coverage'],
    open: {
      cover: {
        path: 'coverage/lcov-report/index.html',
        app: 'Google Chrome'
      }
    }
  });

  // These plugins provide necessary tasks.
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-simple-mocha');
  grunt.loadNpmTasks('grunt-contrib-clean');
  grunt.loadNpmTasks('grunt-open');

  // Default task.
  grunt.registerTask('default', ['jshint', 'simplemocha']);
  grunt.registerTask('cover', ['clean', 'istanbul', 'open:cover']);

  // Run mocha tests while also generating code coverage using istanbul
  grunt.registerTask('istanbul', 'Generate coverage using istanbul from mocha tests', function() {
    var done = this.async();

    var server = grunt.util.spawn({
      cmd: './node_modules/.bin/istanbul',
      args: ['cover', '_mocha', '--']
    }, done);

    server.stdout.pipe(process.stdout);
    server.stderr.pipe(process.stderr);
  });
};
