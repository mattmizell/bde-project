[33mcommit 0aca1df375ca1abd3bc31857f7ca8ec9d4702e6e[m[33m ([m[1;36mHEAD[m[33m -> [m[1;32mmaster[m[33m, [m[1;31morigin/master[m[33m)[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 17:31:33 2025 -0500

    Fix prompt file path for non-OPIS emails and confirm deployment

[33mcommit a1fc55b91c695eca23907cebd59aa64a89a3f69e[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 17:27:39 2025 -0500

    Fix prompt file path for non-OPIS emails and add path logging

[33mcommit 9fcf1ac5a2c243b7bbe90882d72c6bd93307619d[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 17:00:19 2025 -0500

    Fix Supplier extraction for forwarded emails, update prompt for multiple suppliers, fix Wallis email classification

[33mcommit 389c02e9abc54e52972b13c42650cb02c1df0eca[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 16:44:43 2025 -0500

    Update backend: fix Supply extraction for By-Lo, update prompt path, adjust product mappings

[33mcommit ca96c451042b7cf9d7426087087a83234f81c970[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 16:21:09 2025 -0500

    Add CORS middleware to allow requests from frontend

[33mcommit fef10223dc9b9b00067efa4ca6dfddfc6e5cca70[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 16:14:21 2025 -0500

    Fix IDE errors: add aiofiles to requirements, import asyncio, update error message

[33mcommit 6849779c2e4192a1b6a40140d19b9a6a9e3d2aa4[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 16:04:40 2025 -0500

    Fix IDE errors: add aiofiles to requirements, import asyncio, update error message

[33mcommit d5a9feee0aa32836369827c8133b27db21bb55e3[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 15:48:22 2025 -0500

    Fix typo in parser.py (PROMPTS_DIR to PROMPT_DIR)

[33mcommit 05cc8b31cccde13071c6cb6b1038ec6be87df6ae[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 14:27:44 2025 -0500

    Add token tracking and enhance status metrics

[33mcommit 5c3c91d3ebfd1f5450a9518b82135f34f05ef40f[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 14:17:22 2025 -0500

    Update load_mappings to log sheet names and handle variations

[33mcommit d82eec0b882094db038aec49dfd3deb42b6c814c[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 14:12:51 2025 -0500

    Remove initialize_mappings dependency from main.py

[33mcommit 481e30da53dd2e4bb2a0067b3ada213dd03429df[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 14:09:41 2025 -0500

    Update parser.py to use sheet names for mapping categories

[33mcommit bde934ab39c05a984251b4bae2c9d9748b3acdc2[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:55:08 2025 -0500

    Fix status handling to prevent 404 errors during frontend polling

[33mcommit 25ec87a15aeab61ff96cd54b54eab2ce6e2badf1[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:50:24 2025 -0500

    Fix OPIS email classification and handle price conversion from cents to dollars

[33mcommit d3fe15970e86ded62fb0230ffc114dc370badf7b[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:45:33 2025 -0500

    Fix file logging to ensure DEBUG messages are captured

[33mcommit bda1bcf79b89db1b44f1eb6e8a521bbdbc3fa99e[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:38:11 2025 -0500

    Add file logging to output/debug_<process_id>.txt for detailed debugging

[33mcommit ec9065f0b8d33c0d66a8227d3666ea4fa277d9d0[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:31:19 2025 -0500

    Force logging level to DEBUG to capture OPIS parsing details

[33mcommit 3697a3b8187441e088825076192c9cb63dd7818f[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:22:24 2025 -0500

    Fix NameError by adding missing os import in main.py

[33mcommit ffb0c119eb327d96d87de492b7c27dce167e4d61[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:19:19 2025 -0500

    Add detailed logging for OPIS email parsing diagnosis

[33mcommit be2f912b8c6c84d966677f1d3a00896dd9390c2e[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 13:06:45 2025 -0500

    Fix status file creation and unresolved reference in main.py

[33mcommit 73971893090751cb9b00ddf38d1ceba700e00ffe[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 12:43:19 2025 -0500

    Add CORS configuration to enable frontend-backend communication

[33mcommit 566bae9f8f5ae5de048a18f2a399942ca5d5f56f[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 12:40:28 2025 -0500

    Fix: Integrated missing functions and updated main.py with error handling

[33mcommit 16f45524d5ca86180192e5a0d38f545db44885d4[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 11:35:47 2025 -0500

    Fix start-process endpoint to return proper JSON immediately

[33mcommit 930bf9beb9fc28e0ead557ef0137a412059672f0[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 10:54:17 2025 -0500

    Finalize Grok parser, main.py update, and OPIS prompt improvements for deployment

[33mcommit 5760d59ebdf7c84bdf39c2af5627aa2646389392[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 10:04:44 2025 -0500

    Remove CORS middleware and add logging to start-process

[33mcommit e7ad2259d9c576abd96a6074aaee83e53d1ee813[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 09:07:03 2025 -0500

    Confirm 10-second delay in /start-process and processing time logging

[33mcommit f27290a18cd2a63b33b7b3c2e29816b91b3c00c9[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 08:52:59 2025 -0500

    Add CORS middleware to fix cross-origin request issue

[33mcommit 8bf8dd94d78325848a5d010e7ae2c885b0a51e98[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 08:47:58 2025 -0500

    Fix server shutdown and By-Lo Oil parsing issues

[33mcommit 2c939f938f4445e979bce6a1a5793f8ddcf5010e[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 07:29:35 2025 -0500

    Confirm frontend deployment

[33mcommit 106e0f8f3a63ba09a41e9b0da7e6c7dc249194ff[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 06:20:17 2025 -0500

    Update OPIS and Supplier prompts, mappings.xlsx with new entries, and parser.py to apply mappings

[33mcommit 699d07a9cecc8c3a21e34db15fe1c231baacb5a3[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Mon Apr 28 06:15:57 2025 -0500

    Update OPIS and Supplier prompts with examples, and mappings with new entries for OPIS and Luke Oil

[33mcommit ca3820a17e237a3a4b59348b55ce655161aa4931[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 16:11:28 2025 -0500

    Update OPIS Chat Prompt V2 for Big Brain Grok parsing

[33mcommit 991a0a8cd6987e15334e1d8ba073bc9bef588ccf[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 15:53:20 2025 -0500

    Final backend: chat mode only, bulletproof prompts, cleaner download handling

[33mcommit f8a3d5ae5176bd36cb8102ad172049ca3b6258e9[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 15:47:18 2025 -0500

    Update: Bulletproof OPIS and Supplier Chat Prompts

[33mcommit 1bfa55c59f89de7f7fb068f98ec5960c243e79ba[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 15:32:14 2025 -0500

    Update parser: Final Big Brain mode, removed Parse API, cleaned structure

[33mcommit 0b90bd128b1139770862718c837db3a954ca65a4[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 15:09:10 2025 -0500

    Final cleaned parser.py and deployment fixes

[33mcommit a8c03f3048c19ce2188274dd276f964375dfd6c9[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 14:45:22 2025 -0500

    Add sleep after processing to allow frontend polling

[33mcommit f1a89c1475940b4a160ceba9771b4275cb876beb[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 14:39:27 2025 -0500

    Add save_to_csv and save_failed_emails_to_csv functions

[33mcommit 4231b2dee599b3e81b45aaad01c12da5c3385d4c[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 14:31:40 2025 -0500

    Fix main.py imports and background task for new parser

[33mcommit 52b7ad7fe0a6e6c000fe6305d5969ae8c8a0bdf2[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 14:25:25 2025 -0500

    Update parser and prompts for Grok Parse/Chat, Supplier/Terminal handling

[33mcommit 139a09e04215a54dbb0a019ec825e1e635dd4bb4[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 08:00:24 2025 -0500

    Tightened OPIS parsing prompt

[33mcommit b9d2cb8773024737710049c7acdaca7f3a7fcc20[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 07:53:29 2025 -0500

    Improved email parsing: prettify, attachment priority, processed tagging

[33mcommit 64e687a006286950815ebc938216888f5c4454cf[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 07:29:13 2025 -0500

    Final: Improved process_email_with_delay with dynamic OPIS/supplier detection and clean prompts

[33mcommit 9c357fb51a1e29de48c953b42d37b8739ed36d7a[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 07:07:38 2025 -0500

    Full fix: download path using resolved BASE_DIR

[33mcommit 87672a8016da73afe5e234170425c784cb642e35[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 07:03:37 2025 -0500

    Full update: Fix download route and polish backend API

[33mcommit d222fdcae9220e23c0f96cb810b89e43c1d720db[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 06:55:46 2025 -0500

    Deploy Super Prompt v2: Improved parsing accuracy and handling OPIS rack reports

[33mcommit 7495ad7090f408c0a8893c056fd6e7e051dc83ef[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 06:26:08 2025 -0500

    Add .env to .gitignore to protect environment secrets

[33mcommit 9f2a8e70b0e6b02be3d5ddd6489dc21925efcaae[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sun Apr 27 06:23:24 2025 -0500

    Remove .env from repo and add to .gitignore

[33mcommit 3af405094ffa81d147db4e7dce9516bb75b1643c[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 11:14:24 2025 -0500

    Improve supply/terminal separation prompt

[33mcommit aa94ea09adb130fc93b1f7cc35bc871d906e25bc[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 10:54:22 2025 -0500

    Fix: fallback Effective Date from email sent date if missing

[33mcommit fd5cfa9810932b3cafd33cf80e1fe48e0757aa57[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 10:39:13 2025 -0500

    Updated AI prompt for better Supply and Terminal extraction

[33mcommit 5be5e4dc983a17a11c710c8a3f2033f531b6317e[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 10:10:32 2025 -0500

    Fix CORS config and clean main.py

[33mcommit e6ae82b9fd466138ea77c5d1c9c880b5a3070310[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 10:06:29 2025 -0500

    Fix CORS config and clean main.py

[33mcommit 4ef20e817976df76533cfa7f71746c1276cb103c[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 10:01:26 2025 -0500

    Clean CORS setup and prepare for frontend connection

[33mcommit 16da75d6ac987ebf849f132df27dce09ac59af75[m
Author: mattmizell <mattmizell@gmail.com>
Date:   Sat Apr 26 09:22:27 2025 -0500

    Prepare backend for deployment
