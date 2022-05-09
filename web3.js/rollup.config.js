import babel from '@rollup/plugin-babel';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import nodeResolve from '@rollup/plugin-node-resolve';
import replace from '@rollup/plugin-replace';
import {terser} from 'rollup-plugin-terser';

const env = process.env.NODE_ENV;
const extensions = ['.js', '.ts'];

function generateConfig(configType, format) {
  const browser = configType === 'browser';
  const bundle = format === 'iife';

  const config = {
    input: 'src/index.ts',
    plugins: [
      commonjs(),
      nodeResolve({
        browser,
        dedupe: ['bn.js', 'buffer'],
        extensions,
        preferBuiltins: !browser,
      }),
      babel({
        exclude: '**/node_modules/**',
        extensions,
        babelHelpers: bundle ? 'bundled' : 'runtime',
        plugins: bundle ? [] : ['@babel/plugin-transform-runtime'],
      }),
      replace({
        preventAssignment: true,
        values: {
          'process.env.NODE_ENV': JSON.stringify(env),
          'process.env.BROWSER': JSON.stringify(browser),
        },
      }),
    ],
    onwarn: function (warning, rollupWarn) {
      rollupWarn(warning);
      if (warning.code === 'CIRCULAR_DEPENDENCY') {
        throw new Error(
          'Please eliminate the circular dependencies listed ' +
            'above and retry the build',
        );
      }
    },
    treeshake: {
      moduleSideEffects: false,
    },
  };

  if (configType !== 'browser') {
    // Prevent dependencies from being bundled
    config.external = [
      /@babel\/runtime/,
      '@solana/buffer-layout',
      'bn.js',
      'borsh',
      'bs58',
      'buffer',
      'crypto-hash',
      'jayson/lib/client/browser',
      'js-sha3',
      'cross-fetch',
      'rpc-websockets',
      'secp256k1',
      'superstruct',
      'tweetnacl',
    ];
  }

  switch (configType) {
    case 'browser':
      switch (format) {
        case 'iife': {
          config.external = ['http', 'https'];

          config.output = [
            {
              file: 'lib/index.iife.js',
              format: 'iife',
              name: 'solanaWeb3',
              sourcemap: true,
            },
            {
              file: 'lib/index.iife.min.js',
              format: 'iife',
              name: 'solanaWeb3',
              sourcemap: true,
              plugins: [terser({mangle: false, compress: false})],
            },
          ];

          break;
        }
        default: {
          config.output = [
            {
              file: 'lib/index.browser.cjs.js',
              format: 'cjs',
              sourcemap: true,
            },
            {
              file: 'lib/index.browser.esm.js',
              format: 'es',
              sourcemap: true,
            },
          ];

          // Prevent dependencies from being bundled
          config.external = [
            /@babel\/runtime/,
            '@solana/buffer-layout',
            'bn.js',
            'borsh',
            'bs58',
            'buffer',
            'crypto-hash',
            'http',
            'https',
            'jayson/lib/client/browser',
            'js-sha3',
            'rpc-websockets',
            'secp256k1',
            'superstruct',
            'tweetnacl',
          ];

          break;
        }
      }

      // TODO: Find a workaround to avoid resolving the following JSON file:
      // `node_modules/secp256k1/node_modules/elliptic/package.json`
      config.plugins.push(json());

      break;
    case 'node':
      config.output = [
        {
          file: 'lib/index.cjs.js',
          format: 'cjs',
          sourcemap: true,
        },
        {
          file: 'lib/index.esm.js',
          format: 'es',
          sourcemap: true,
        },
      ];
      break;
    default:
      throw new Error(`Unknown configType: ${configType}`);
  }

  return config;
}

export default [
  generateConfig('node'),
  generateConfig('browser'),
  generateConfig('browser', 'iife'),
];
