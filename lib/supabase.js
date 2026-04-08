require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
// Prefer service role key for server-side operations (full DB access, bypasses RLS)
// Falls back to anon key for backward compatibility with existing deployments
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.warn('Supabase URL or Key is missing in environment variables.');
}

if (!process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.warn('[Analytics API] Using SUPABASE_ANON_KEY — set SUPABASE_SERVICE_ROLE_KEY for production.');
}

const supabase = createClient(supabaseUrl, supabaseKey);

module.exports = supabase;
